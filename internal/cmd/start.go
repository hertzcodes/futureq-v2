/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	stdLogger "log"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/config"
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

	app, err := app.Init(cfg, logger)
	if err != nil {
		logger.Fatal("failed to init app", zap.Error(err))
	}


	app.WithGRPC()

	go func() {
		if err := app.WithGracefulShutdown(); err != nil {
			logger.Error("graceful shutdown error", zap.Error(err))
		}
	}()
}
