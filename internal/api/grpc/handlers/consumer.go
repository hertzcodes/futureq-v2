package handlers

import (
	proto "github.com/futureq-io/futureq/proto/go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConsumerHandler implements proto.FutureQConsumerServer.
type ConsumerHandler struct {
	proto.UnimplementedFutureQConsumerServer
	logger *zap.Logger
}

// NewConsumerHandler returns an initialised ConsumerHandler.
func NewConsumerHandler(logger *zap.Logger) *ConsumerHandler {
	return &ConsumerHandler{
		logger: logger.Named("consumer"),
	}
}

// Subscribe handles a bidirectional stream where the server pushes
// QueueMessage items to the client and the client replies with AckRequest
// messages to confirm (or reject) each delivery.
//
// The server drives message delivery; the client drives acknowledgements.
func (h *ConsumerHandler) Subscribe(stream grpc.BidiStreamingServer[proto.AckRequest, proto.QueueMessage]) error {
	// TODO: implement subscribe / ack logic.
	return status.Errorf(codes.Unimplemented, "Subscribe is not yet implemented")
}
