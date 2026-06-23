package handlers

import (
	"context"
	"errors"
	"io"

	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/dispatcher"
	pb "github.com/futureq-io/protocol/proto/go"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConsumerHandler implements proto.FutureQConsumerServer.
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

// Subscribe handles a bidirectional stream where the server pushes
// QueueMessage items to the client and the client replies with AckRequest
// messages to confirm (or reject) each delivery.
func (h *ConsumerHandler) Subscribe(stream grpc.BidiStreamingServer[pb.AckRequest, pb.QueueMessage]) error {
	if app.A.NodeHost != nil {
		shardID := app.A.Config().Raft.ClusterID
		leaderID, _, valid, err := app.A.NodeHost.GetLeaderID(shardID)
		if err != nil || !valid || leaderID != app.A.Config().Raft.NodeID {
			h.logger.Warn("rejecting consumer connection, not the leader")
			return status.Errorf(codes.FailedPrecondition, "node is not the cluster leader")
		}
	}

	consumerID := uuid.New().String()
	ch := make(chan *pb.QueueMessage, 1024)
	h.hub.Register(consumerID, ch)
	defer h.hub.Unregister(consumerID)

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	errCh := make(chan error, 2)

	// Sender goroutine
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

	// Receiver goroutine
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					errCh <- nil
				} else {
					errCh <- err
				}
				return
			}

			if req.Success {
				h.deleter.MarkDeleted(req.DeliveryTag)
			}
		}
	}()

	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && err != io.EOF {
		h.logger.Error("consumer stream ended with error", zap.Error(err), zap.String("id", consumerID))
		return status.Errorf(codes.Internal, "stream error: %v", err)
	}

	return nil
}
