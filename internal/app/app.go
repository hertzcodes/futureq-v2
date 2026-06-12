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

	"github.com/lni/dragonboat/v4"
	raftconfig "github.com/lni/dragonboat/v4/config"
	"go.uber.org/zap"

	"github.com/futureq-io/futureq/internal/config"
	"github.com/futureq-io/futureq/internal/raft"
	"github.com/futureq-io/futureq/internal/storage"
)

const gracefulShutdownTimeout = 10 * time.Second

var A *App

type App struct {
	cfg      *config.Config
	Pebble   *storage.Pebble
	NodeHost *dragonboat.NodeHost
	Ctx      context.Context
	// ShutCtx is the 10-second shutdown window context. It is populated by
	// WithGracefulShutdown immediately before a.Ctx is cancelled, so any
	// goroutine watching a.Ctx.Done() can safely read ShutCtx.
	ShutCtx context.Context
	cancel  context.CancelCauseFunc
	Logger  *zap.Logger
	wg      sync.WaitGroup
}

func Init(cfg *config.Config, logger *zap.Logger) (*App, error) {
	a := &App{
		cfg:    cfg,
		Logger: logger.Named("app"),
	}

	a.Ctx, a.cancel = context.WithCancelCause(context.Background())

	pebble, err := storage.NewPebble(cfg.Storage.Pebble, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pebble storage: %w", err)
	}

	a.Pebble = pebble

	if cfg.Raft.NodeID > 0 {
		rttMs := cfg.Raft.RTTMillisecond
		if rttMs == 0 {
			rttMs = 200 // sane default: calibrates heartbeat and election timeouts
		}

		nhc := raftconfig.NodeHostConfig{
			WALDir:         cfg.Raft.DataPath,
			NodeHostDir:    cfg.Raft.DataPath,
			RTTMillisecond: rttMs,
			RaftAddress:    cfg.Raft.ListenAddress,
		}

		nh, err := dragonboat.NewNodeHost(nhc)
		if err != nil {
			return nil, fmt.Errorf("failed to create dragonboat nodehost: %w", err)
		}
		a.NodeHost = nh

		snapEntries := cfg.Raft.SnapshotEntries
		if snapEntries == 0 {
			snapEntries = 10000
		}
		compOverhead := cfg.Raft.CompactionOverhead
		if compOverhead == 0 {
			compOverhead = 5000
		}

		rc := raftconfig.Config{
			ReplicaID:          cfg.Raft.NodeID,
			ShardID:            cfg.Raft.ClusterID,
			ElectionRTT:        10,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    snapEntries,
			CompactionOverhead: compOverhead,
		}

		members := make(map[uint64]dragonboat.Target)
		for k, v := range cfg.Raft.InitialMembers {
			members[k] = dragonboat.Target(v)
		}

		if err := nh.StartOnDiskReplica(members, false, raft.NewEventStateMachineFactory(pebble.DB, logger), rc); err != nil {
			return nil, fmt.Errorf("failed to start raft cluster: %w", err)
		}
	}

	A = a

	return a, nil
}

// Config returns the application configuration.
func (a *App) Config() *config.Config {
	return a.cfg
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
	a.Logger.Info("received interrupt, shutting down gracefully...")

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
		a.Logger.Info("graceful shutdown completed before timeout")
	case <-shutCtx.Done():
		a.Logger.Warn("graceful shutdown timeout exceeded, forcing exit")
	}

	if a.NodeHost != nil {
		a.Logger.Info("closing Dragonboat NodeHost...")
		a.NodeHost.Close()
		a.Logger.Info("Dragonboat NodeHost closed successfully")
	}

	if err := a.Pebble.DB.Flush(); err != nil {
		a.Logger.Error("failed to flush pebble on shutdown", zap.Error(err))
	}

	// 4. Safely close Pebble DB.
	if a.Pebble != nil && a.Pebble.DB != nil {
		a.Logger.Info("closing Pebble DB...")
		if err := a.Pebble.DB.Close(); err != nil {
			a.Logger.Error("failed to close Pebble DB", zap.Error(err))
		} else {
			a.Logger.Info("Pebble DB closed successfully")
		}
	}

	return nil
}
