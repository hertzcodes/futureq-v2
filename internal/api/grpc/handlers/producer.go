package handlers

import (
	"io"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/repository"
	pb "github.com/futureq-io/futureq/proto/go"
)

// ProducerHandler implements proto.FutureQProducerServer.
type ProducerHandler struct {
	pb.UnimplementedFutureQProducerServer
	logger         *zap.Logger
	eventRepo      *repository.EventRepository
	timeBucketSize time.Duration
}

// NewProducerHandler returns an initialised ProducerHandler.
func NewProducerHandler(logger *zap.Logger) *ProducerHandler {
	bucketSize := app.A.Config().Storage.TimeBucketSize

	ph := &ProducerHandler{
		logger:         logger.Named("producer"),
		timeBucketSize: bucketSize,
	}

	eventRepo, err := repository.NewEventRepository(app.A.Pebble.DB)
	if err != nil {
		ph.logger.Fatal("failed to init event repo", zap.Error(err))
	}

	ph.eventRepo = eventRepo

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
			ph.logger.Error("failed to marshal request", zap.String("message_id", req.MessageId), zap.Error(err))

			if err := stream.Send(&pb.StreamPublishAck{
				MessageId:    req.MessageId,
				Success:      false,
				ErrorMessage: "internal error: failed to serialize message",
			}); err != nil {
				ph.logger.Error("failed to send ack", zap.Error(err))
			}

			continue
		}

		executeAt := req.ExecuteAtUnixMs
		bucket := calculateBucket(executeAt, ph.timeBucketSize)
		err = ph.eventRepo.Store(bucket, data)

		ack := &pb.StreamPublishAck{
			MessageId: req.MessageId,
		}

		if err != nil {
			ph.logger.Error("failed to store event", zap.String("message_id", req.MessageId), zap.Error(err))
			ack.Success = false
			ack.ErrorMessage = "failed to persist message to database"
		} else {
			ack.Success = true
		}

		if err := stream.Send(ack); err != nil {
			ph.logger.Error("failed to send ack", zap.String("message_id", req.MessageId), zap.Error(err))
			return status.Errorf(codes.Internal, "failed to send ack: %v", err)
		}
	}
}

func calculateBucket(executeAt int64, bucketSize time.Duration) uint64 {
	if executeAt <= 0 {
		return 0
	}

	bucketSizeMs := bucketSize.Milliseconds()
	if bucketSizeMs > 0 {
		k := (executeAt + bucketSizeMs - 1) / bucketSizeMs
		return uint64(k * bucketSizeMs)
	}

	return uint64(executeAt)
}
