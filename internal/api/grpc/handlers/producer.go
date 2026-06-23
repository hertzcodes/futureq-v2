package handlers

import (
	"context"
	"io"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/raft"
	"github.com/futureq-io/futureq/internal/repository"
	"github.com/futureq-io/futureq/pkg/utils"
	pb "github.com/futureq-io/protocol/proto/go"
)

// ProducerHandler implements proto.FutureQProducerServer.
type ProducerHandler struct {
	pb.UnimplementedFutureQProducerServer
	logger         *zap.Logger
	eventRepo      *repository.EventRepository
	timeBucketSize time.Duration
}

// NewProducerHandler returns an initialised ProducerHandler.
// In Raft mode the handler never writes directly to Pebble; all writes go
// through SyncPropose → state machine → EventRepository.StoreWithBatch.
// The local eventRepo is therefore only initialised in non-Raft (single-node)
// mode to avoid an unnecessary Pebble read and a redundant lastID counter.
func NewProducerHandler(logger *zap.Logger) *ProducerHandler {
	bucketSize := app.A.Config().Storage.TimeBucketSize

	ph := &ProducerHandler{
		logger:         logger.Named("producer"),
		timeBucketSize: bucketSize,
	}

	// Only needed in non-Raft (standalone) mode.
	if app.A.NodeHost == nil {
		eventRepo, err := repository.NewEventRepository(app.A.Pebble.DB, ph.logger)
		if err != nil {
			ph.logger.Fatal("failed to init event repo", zap.Error(err))
		}
		ph.eventRepo = eventRepo
	}

	return ph
}

// PublishStream handles a bidirectional stream where clients send
// StreamPublishRequest messages and receive StreamPublishAck responses.
//
// The client sends a batch of scheduled messages; the server acknowledges
// each one individually so the client can track per-message delivery.
func (ph *ProducerHandler) PublishStream(stream grpc.BidiStreamingServer[pb.StreamPublishRequest, pb.StreamPublishAck]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			ph.logger.Error("failed to receive from stream", zap.Error(err))
			return status.Errorf(codes.Internal, "stream read error: %v", err)
		}

		data, err := proto.Marshal(req)
		if err != nil {
			ph.logger.Error("failed to marshal request", zap.String("topic", req.GetTopic()), zap.Error(err))

			if err := stream.Send(&pb.StreamPublishAck{
				Success:      false,
				ErrorMessage: "internal error: failed to serialize message",
			}); err != nil {
				ph.logger.Error("failed to send ack", zap.Error(err))
			}

			continue
		}

		executeAt := req.ExecuteAtUnixMs
		bucket := utils.CalculateBucket(executeAt, ph.timeBucketSize)
		if app.A.NodeHost != nil {
			shardID := app.A.Config().Raft.ClusterID
			leaderID, _, valid, errL := app.A.NodeHost.GetLeaderID(shardID)
			if errL != nil || !valid || leaderID != app.A.Config().Raft.NodeID {
				ph.logger.Warn("rejecting write, not the leader", zap.Uint64("leader", leaderID), zap.Error(errL))
				if err := stream.Send(&pb.StreamPublishAck{
					Success:      false,
					ErrorMessage: "node is not the cluster leader",
				}); err != nil {
					ph.logger.Error("failed to send ack", zap.Error(err))
				}
				continue
			}

			cmd := &raft.Command{
				Type:   raft.StoreEventCmd,
				Bucket: bucket,
				Data:   data,
			}
			cmdBytes, err2 := raft.MarshalCommand(cmd)
			if err2 != nil {
				err = err2
			} else {
				ctx, cancel := context.WithTimeout(stream.Context(), 5*time.Second)
				session := app.A.NodeHost.GetNoOPSession(app.A.Config().Raft.ClusterID)
				_, err = app.A.NodeHost.SyncPropose(ctx, session, cmdBytes)
				cancel()
			}
		} else {
			err = ph.eventRepo.Store(bucket, data)
		}

		ack := &pb.StreamPublishAck{}

		if err != nil {
			ph.logger.Error("failed to store event", zap.String("topic", req.GetTopic()), zap.Error(err))
			ack.Success = false
			ack.ErrorMessage = "failed to persist message to database"
		} else {
			ack.Success = true
		}

		if err := stream.Send(ack); err != nil {
			ph.logger.Error("failed to send ack", zap.String("topic", req.GetTopic()), zap.Error(err))
			return status.Errorf(codes.Internal, "failed to send ack: %v", err)
		}
	}
}
