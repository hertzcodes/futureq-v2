package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/futureq-io/futureq/internal/config"
	"github.com/futureq-io/futureq/internal/storage"
	"go.uber.org/zap"
)

const gracefulShutdownTimeout = 10 * time.Second

var A *App

type App struct {
	cfg    *config.Config
	Pebble *storage.Pebble
	Ctx    context.Context
	// ShutCtx is the 10-second shutdown window context. It is populated by
	// WithGracefulShutdown immediately before a.Ctx is cancelled, so any
	// goroutine watching a.Ctx.Done() can safely read ShutCtx.
	ShutCtx context.Context
	cancel  context.CancelCauseFunc
	logger  *zap.Logger
	wg      sync.WaitGroup
}

func Init(cfg *config.Config, logger *zap.Logger) (*App, error) {
	a := &App{
		cfg:    cfg,
		logger: logger.Named("app"),
	}

	a.Ctx, a.cancel = context.WithCancelCause(context.Background())

	pebble, err := storage.NewPebble(cfg.Storage.Pebble, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pebble storage: %w", err)
	}

	a.Pebble = pebble

	A = a

	return a, nil
}

// RegisterComponentWithShutdown increments the application wait group to track active components during shutdown.
func (a *App) RegisterComponentWithShutdown() {
	a.wg.Add(1)
}

// ComponentShutdownDone decrements the application wait group.
func (a *App) ComponentShutdownDone() {
	a.wg.Done()
}

func (a *App) WithGracefulShutdown() error {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigterm)

	<-sigterm
	a.logger.Info("received interrupt, shutting down gracefully...")

	// 1. Create the shared shutdown window.
	// We MUST use context.Background() as the parent, because if we use a.Ctx,
	// calling a.cancel() will immediately cancel shutCtx, causing an instant timeout.
	shutCtx, shutCancel := context.WithTimeoutCause(
		context.Background(),
		gracefulShutdownTimeout,
		errors.New("graceful shutdown timeout exceeded"),
	)
	defer shutCancel()

	a.ShutCtx = shutCtx

	// 2. Signal all components to start winding down.
	a.cancel(errors.New("graceful shutdown triggered"))

	// 3. Wait for all registered components to finish, or the timeout to expire.
	waitDone := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		a.logger.Info("graceful shutdown completed before timeout")
	case <-shutCtx.Done():
		a.logger.Warn("graceful shutdown timeout exceeded, forcing exit")
	}

	// 4. Safely close Pebble DB.
	if a.Pebble != nil && a.Pebble.DB != nil {
		a.logger.Info("closing Pebble DB...")
		if err := a.Pebble.DB.Close(); err != nil {
			a.logger.Error("failed to close Pebble DB", zap.Error(err))
		} else {
			a.logger.Info("Pebble DB closed successfully")
		}
	}

	return nil
}
