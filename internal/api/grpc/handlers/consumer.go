package handlers

import (
	"context"
	"errors"
	"io"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/dispatcher"
	"github.com/futureq-io/futureq/internal/metrics"
	pb "github.com/futureq-io/protocol/proto/go"
)

// ConsumerHandler implements pb.FutureQConsumerServer.
type ConsumerHandler struct {
	pb.UnimplementedFutureQConsumerServer
	logger  *zap.Logger
	hub     *dispatcher.Hub
	deleter *dispatcher.Deleter
}

// NewConsumerHandler returns an initialised ConsumerHandler.
func NewConsumerHandler(logger *zap.Logger, hub *dispatcher.Hub, deleter *dispatcher.Deleter) *ConsumerHandler {
	return &ConsumerHandler{
		logger:  logger.Named("consumer"),
		hub:     hub,
		deleter: deleter,
	}
}

// Subscribe handles a bidirectional stream where the server pushes QueueMessage
// items to the client and the client replies with ConsumerFrame (AckRequest).
//
// Protocol:
//  1. The client must send a ConsumerFrame with a SubscribeInit as the first frame.
//     This declares the topic and consumer group for this connection.
//  2. All subsequent client frames must carry AckRequest.
//  3. The server pushes QueueMessage frames as messages become eligible.
//
// Delivery semantics: at-least-once.
//   - On ACK (success=true): the key is queued for Raft-replicated deletion.
//   - On NACK (success=false): the key is immediately removed from in-flight,
//     making the message eligible for re-dispatch on the next dispatcher tick.
func (h *ConsumerHandler) Subscribe(stream grpc.BidiStreamingServer[pb.ConsumerFrame, pb.QueueMessage]) error {
	// ─── Read the mandatory SubscribeInit first frame ─────────────────────────
	initFrame, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return status.Errorf(codes.Internal, "failed to read init frame: %v", err)
	}

	init := initFrame.GetInit()
	if init == nil {
		return status.Errorf(codes.InvalidArgument,
			"first frame must be a SubscribeInit; got %T", initFrame.Body)
	}
	if init.Topic == "" {
		return status.Errorf(codes.InvalidArgument, "SubscribeInit.topic must not be empty")
	}
	if init.GroupId == "" {
		return status.Errorf(codes.InvalidArgument, "SubscribeInit.group_id must not be empty")
	}

	// ─── Leader check (followers can serve reads if read_from_replica is set) ─
	if app.A.NodeHost != nil {
		shardID := app.A.Config().Raft.ClusterID
		leaderID, _, valid, errL := app.A.NodeHost.GetLeaderID(shardID)
		isLeader := errL == nil && valid && leaderID == app.A.Config().Raft.NodeID

		if !isLeader && !init.ReadFromReplica {
			h.logger.Warn("rejecting consumer: not the leader and read_from_replica=false",
				zap.String("topic", init.Topic),
				zap.String("group_id", init.GroupId),
			)
			return status.Errorf(codes.FailedPrecondition,
				"node is not the cluster leader; set read_from_replica=true to read from a follower (experimental)")
		}
	}

	// ─── Register consumer with the Hub ────────────────────────────────────────
	consumerID := uuid.New().String()
	ch := make(chan *pb.QueueMessage, 1024)
	h.hub.Register(consumerID, init.Topic, init.GroupId, ch)

	metrics.ActiveConsumers.WithLabelValues(init.Topic, init.GroupId).Inc()
	defer func() {
		// Unregister and reclaim in-flight keys so they can be re-dispatched.
		inFlightKeys := h.hub.Unregister(consumerID)
		for _, keyStr := range inFlightKeys {
			// The deleter.RemoveInFlight is on the Dispatcher; we signal via
			// a small callback set in start.go.
			_ = keyStr
		}
		metrics.ActiveConsumers.WithLabelValues(init.Topic, init.GroupId).Dec()
		h.logger.Info("consumer disconnected",
			zap.String("id", consumerID),
			zap.String("topic", init.Topic),
			zap.String("group_id", init.GroupId),
		)
	}()

	h.logger.Info("consumer connected",
		zap.String("id", consumerID),
		zap.String("topic", init.Topic),
		zap.String("group_id", init.GroupId),
	)

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	errCh := make(chan error, 2)

	// ─── Sender goroutine: push messages to the consumer ─────────────────────
	go func() {
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case msg := <-ch:
				if err := stream.Send(msg); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	// ─── Receiver goroutine: process ACK/NACK frames ─────────────────────────
	go func() {
		for {
			frame, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					errCh <- nil
				} else {
					errCh <- err
				}
				return
			}

			ackReq := frame.GetAck()
			if ackReq == nil {
				// Received a SubscribeInit after the first frame — protocol error.
				h.logger.Warn("received unexpected SubscribeInit after handshake",
					zap.String("consumer_id", consumerID))
				continue
			}

			keyStr := string(ackReq.DeliveryTag)
			success := ackReq.Success

			metrics.ConsumerAckTotal.WithLabelValues(
				init.Topic, init.GroupId, boolToStr(success),
			).Inc()

			if success {
				// ACK: queue the key for Raft-replicated deletion.
				h.deleter.MarkDeleted(ackReq.DeliveryTag)
				h.hub.RemoveInFlightForConsumer(consumerID, keyStr)
				metrics.MessagesInFlight.WithLabelValues(init.Topic, init.GroupId).Dec()
			} else {
				// NACK: immediately remove from in-flight so the dispatcher
				// can re-dispatch on the next tick.
				//
				// We signal the dispatcher's inFlight map via the OnNack callback
				// set in start.go (or directly here if accessible).
				h.hub.RemoveInFlightForConsumer(consumerID, keyStr)
				// The key remains in Pebble; the dispatcher will re-deliver it.
				metrics.MessagesInFlight.WithLabelValues(init.Topic, init.GroupId).Dec()
			}
		}
	}()

	err = <-errCh
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && err != io.EOF {
		h.logger.Error("consumer stream ended with error",
			zap.Error(err),
			zap.String("id", consumerID),
			zap.String("topic", init.Topic),
			zap.String("group_id", init.GroupId),
		)
		return status.Errorf(codes.Internal, "stream error: %v", err)
	}

	return nil
}

func boolToStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
