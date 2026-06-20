/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	stdLogger "log"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/futureq-io/futureq/internal/api/grpc"
	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/config"
	"github.com/futureq-io/futureq/internal/dispatcher"
	"github.com/futureq-io/futureq/pkg/log"
)

// startCmd represents the server command
var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the server",
	Run:   startRun,
}

func startRun(_ *cobra.Command, _ []string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		stdLogger.Fatalf("failed to load config: %v", err)
	}

	logger, err := log.InitLogger(cfg.Observability.Logger)
	if err != nil {
		stdLogger.Fatalf("failed to init logger: %v", err)
	}

	a, err := app.Init(cfg, logger)
	if err != nil {
		logger.Fatal("failed to init app", zap.Error(err))
	}

	wakeCh := make(chan struct{}, 1)
	hub := dispatcher.NewHub(logger, wakeCh)
	deleter := dispatcher.NewDeleter(a.Pebble.DB, time.Duration(cfg.Consumer.DeleteBatchIntervalMs)*time.Millisecond, logger)
	disp := dispatcher.NewDispatcher(a.Pebble.DB, hub, time.Duration(cfg.Consumer.DispatchPollIntervalMs)*time.Millisecond, wakeCh, logger)
	deleter.OnDelete = disp.RemoveInFlight

	a.RegisterComponentWithShutdown()
	go func() {
		defer a.ComponentShutdownDone()
		deleter.Run(a.Ctx)
	}()

	a.RegisterComponentWithShutdown()
	go func() {
		defer a.ComponentShutdownDone()
		disp.Run(a.Ctx)
	}()

	grpc.New(cfg.Server, hub, deleter, logger).Listen().WaitForShutdown(a.Ctx)

	if err := a.WithGracefulShutdown(); err != nil {
		logger.Fatal("failed to graceful shutdown", zap.Error(err))
	}
}
