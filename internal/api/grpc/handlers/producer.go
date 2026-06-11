package handlers

import (
	proto "github.com/futureq-io/futureq/proto/go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProducerHandler implements proto.FutureQProducerServer.
type ProducerHandler struct {
	proto.UnimplementedFutureQProducerServer
	logger *zap.Logger
}

// NewProducerHandler returns an initialised ProducerHandler.
func NewProducerHandler(logger *zap.Logger) *ProducerHandler {
	return &ProducerHandler{
		logger: logger.Named("producer"),
	}
}

// PublishStream handles a bidirectional stream where clients send
// StreamPublishRequest messages and receive StreamPublishAck responses.
//
// The client sends a batch of scheduled messages; the server acknowledges
// each one individually so the client can track per-message delivery.
func (h *ProducerHandler) PublishStream(stream grpc.BidiStreamingServer[proto.StreamPublishRequest, proto.StreamPublishAck]) error {
	// TODO: implement publish logic.
	return status.Errorf(codes.Unimplemented, "PublishStream is not yet implemented")
}
