package storage

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/futureq-io/futureq/internal/config"
	"go.uber.org/zap"
)

type Pebble struct {
	db     *pebble.DB
	logger *zap.Logger
}

func NewPebble(cfg config.Pebble, logger *zap.Logger) (*Pebble, error) {
	pebbleLogger := logger.Named("storage").With(
		zap.String("engine", "pebble"),
	)

	cacheSize := cfg.CacheSizeMB * 1024 * 1024
	if cacheSize <= 0 {
		cacheSize = 64 * 1024 * 1024
	}

	cache := pebble.NewCache(cacheSize)
	// this somehow prevents memory leaks in the opts
	defer cache.Unref()

	dbOpts := &pebble.Options{
		DisableWAL:   cfg.DisableWAL,
		Logger:       pebbleLogger.Sugar(),
		Cache:        cache,
		MemTableSize: cfg.InMemTableSizeMB * 1024 * 1024,
		// EventListener:,
	}

	if cfg.DataPath == "" {
		dbOpts.FS = vfs.NewMem()
		pebbleLogger.Info("Initializing Pebble DB in memory", zap.Bool("persist", false))
	} else {
		pebbleLogger.Info("Initializing Pebble DB", zap.Bool("persist", true))
	}

	db, err := pebble.Open(cfg.DataPath, dbOpts)
	if err != nil {
		return nil, err
	}

	return &Pebble{
		db:     db,
		logger: logger,
	}, nil
}
