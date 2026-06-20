package dispatcher

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
)

type Deleter struct {
	db       *pebble.DB
	logger   *zap.Logger
	interval time.Duration
	pending  [][]byte
	mu       sync.Mutex
	OnDelete func(key []byte)
}

func NewDeleter(db *pebble.DB, interval time.Duration, logger *zap.Logger) *Deleter {
	return &Deleter{
		db:       db,
		logger:   logger.Named("deleter"),
		interval: interval,
		pending:  make([][]byte, 0, 1024),
	}
}

func (d *Deleter) MarkDeleted(key []byte) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	d.mu.Lock()
	d.pending = append(d.pending, keyCopy)
	d.mu.Unlock()
}

func (d *Deleter) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.flush()
			return
		case <-ticker.C:
			d.flush()
		}
	}
}

func (d *Deleter) flush() {
	d.mu.Lock()
	if len(d.pending) == 0 {
		d.mu.Unlock()
		return
	}

	keysToFlush := d.pending
	d.pending = make([][]byte, 0, 1024)
	d.mu.Unlock()

	batch := d.db.NewBatch()
	defer batch.Close()

	for _, key := range keysToFlush {
		if err := batch.Delete(key, nil); err != nil {
			d.logger.Error("failed to mark key for deletion", zap.Error(err))
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		d.logger.Error("failed to commit delete batch", zap.Error(err))
	} else {
		d.logger.Debug("flushed delete batch", zap.Int("count", len(keysToFlush)))
		if d.OnDelete != nil {
			for _, key := range keysToFlush {
				d.OnDelete(key)
			}
		}
	}
}
