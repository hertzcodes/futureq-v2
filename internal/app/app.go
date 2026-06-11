package app

import (
	"fmt"

	"github.com/futureq-io/futureq/internal/config"
	"github.com/futureq-io/futureq/internal/storage"
	"go.uber.org/zap"
)

type app struct {
	db *storage.Pebble
}

func Init(cfg *config.Config, logger *zap.Logger) (*app, error) {
	var a *app

	pebble, err := storage.NewPebble(cfg.Storage.Pebble, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pebble storage: %w", err)
	}

	a.db = pebble

	return a, nil
}
