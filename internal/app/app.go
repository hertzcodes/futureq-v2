package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcapi "github.com/futureq-io/futureq/internal/api/grpc"
	"github.com/futureq-io/futureq/internal/config"
	"github.com/futureq-io/futureq/internal/storage"
	"go.uber.org/zap"
)

const gracefulShutdownTimeout = 10 * time.Second

type app struct {
	cfg        *config.Config
	db         *storage.Pebble
	grpcServer *grpcapi.Server
	ctx        context.Context
	cancel     context.CancelCauseFunc
	logger     *zap.Logger
}

func Init(cfg *config.Config, logger *zap.Logger) (*app, error) {
	a := &app{
		cfg:    cfg,
		logger: logger.Named("app"),
	}

	a.ctx, a.cancel = context.WithCancelCause(context.Background())

	pebble, err := storage.NewPebble(cfg.Storage.Pebble, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pebble storage: %w", err)
	}

	a.db = pebble
	a.grpcServer = grpcapi.New(cfg.Server, logger)

	return a, nil
}

// WithGRPC launches the gRPC server in a background goroutine and forwards any
// serve error back through the returned channel. The caller should select on
// that channel alongside other termination signals.
func (a *app) WithGRPC() {
	go func() {
		if err := a.grpcServer.Listen(a.cfg.Server.Listen); err != nil {
			a.logger.Fatal("grpc server error", zap.Error(err))
		}
	}()
}

// WithGracefulShutdown blocks until SIGINT or SIGTERM is received, then
// cancels the application context and gives the gRPC server up to
// gracefulShutdownTimeout to finish in-flight RPCs before returning.
func (a *app) WithGracefulShutdown() error {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigterm)

	<-sigterm
	a.logger.Info("received interrupt, shutting down gracefully...")

	a.cancel(errors.New("graceful shutdown triggered"))

	shutCtx, shutCancel := context.WithTimeoutCause(
		context.Background(),
		gracefulShutdownTimeout,
		errors.New("graceful shutdown timeout exceeded"),
	)

	defer shutCancel()

	a.grpcServer.Shutdown(shutCtx)

	a.logger.Info("server exited properly")
	return nil
}
