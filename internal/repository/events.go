package repository

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"

	"github.com/futureq-io/futureq/pkg/utils"
)

var eventsLastIDKey = []byte("metadata/event-repo/last-id")

// EventRepository manages the monotonic event ID counter stored in Pebble.
type EventRepository struct {
	db     *pebble.DB
	logger *zap.Logger
	lastID uint64
}

func NewEventRepository(db *pebble.DB, logger *zap.Logger) (*EventRepository, error) {
	repo := &EventRepository{
		db:     db,
		logger: logger,
	}

	val, closer, err := db.Get(eventsLastIDKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			repo.lastID = 0
		} else {
			return nil, err
		}
	} else {
		repo.lastID = binary.BigEndian.Uint64(val)
		_ = closer.Close()
	}

	return repo, nil
}

// StoreWithBatch marshals msg and adds it to an existing Pebble batch.
// It returns the generated 24-byte Pebble key for the caller to use as a delivery_tag.
func (er *EventRepository) StoreWithBatch(b *pebble.Batch, bucket, topicHash uint64, value []byte) ([]byte, error) {
	nextID := atomic.AddUint64(&er.lastID, 1)

	key := utils.EventKey(bucket, topicHash, nextID)

	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, nextID)
	if err := b.Set(eventsLastIDKey, idBytes, nil); err != nil {
		return nil, err
	}

	if err := b.Set(key, value, nil); err != nil {
		return nil, err
	}

	return key, nil
}

func (er *EventRepository) DeleteWithBatch(b *pebble.Batch, key []byte) error {
	return b.Delete(key, nil)
}
