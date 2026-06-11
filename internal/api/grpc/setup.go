package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/futureq-io/futureq/internal/api/grpc/handlers"
	"github.com/futureq-io/futureq/internal/config"
	proto "github.com/futureq-io/futureq/proto/go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Server wraps a *grpc.Server and exposes lifecycle methods.
type Server struct {
	srv    *grpc.Server
	logger *zap.Logger
}

// New creates a fully configured gRPC server and registers all service
// handlers. No network socket is opened yet; call Listen to do that.
func New(cfg config.Server, logger *zap.Logger) *Server {
	log := logger.Named("grpc_server")

	srv := grpc.NewServer(
		// Honour the operator-supplied connection ceiling.
		grpc.MaxConcurrentStreams(cfg.MaxConns),

		// Keepalive enforcement: drop clients that ignore pings.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),

		// Keepalive server-side parameters.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     30 * time.Second,
			MaxConnectionAge:      2 * time.Minute,
			MaxConnectionAgeGrace: 10 * time.Second,
			Time:                  10 * time.Second,
			Timeout:               cfg.Timeout,
		}),
	)

	// Register service implementations.
	proto.RegisterFutureQProducerServer(srv, handlers.NewProducerHandler(log))
	proto.RegisterFutureQConsumerServer(srv, handlers.NewConsumerHandler(log))

	return &Server{
		srv:    srv,
		logger: log,
	}
}

// Listen binds the TCP listener and blocks serving until the underlying
// gRPC server is stopped. Call Shutdown to stop it gracefully.
func (s *Server) Listen(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("grpc: failed to bind %s: %w", address, err)
	}

	s.logger.Info("gRPC server listening", zap.String("address", address))

	if err := s.srv.Serve(lis); err != nil {
		return err
	}

	return nil
}

// Shutdown attempts a graceful stop within the deadline carried by ctx.
// If the deadline expires before all RPCs finish, it hard-stops the server.
func (s *Server) Shutdown(ctx context.Context) {
	s.logger.Info("gRPC server: initiating graceful shutdown")

	done := make(chan struct{})
	go func() {
		s.srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("gRPC server: stopped gracefully")
	case <-ctx.Done():
		s.logger.Warn("gRPC server: graceful shutdown timed out, forcing stop")
		s.srv.Stop()
	}
}
