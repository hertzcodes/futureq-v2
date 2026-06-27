package repository

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/futureq-io/futureq/pkg/utils"
	storagepb "github.com/futureq-io/protocol/proto/go/storage"
)

var eventsLastIDKey = []byte("metadata/event-repo/last-id")

// EventRepository manages the monotonic event ID counter stored in Pebble.
type EventRepository struct {
	db         *pebble.DB
	logger     *zap.Logger
	lastID     uint64
	bucketSize time.Duration
}

func NewEventRepository(db *pebble.DB, logger *zap.Logger, bucketSize time.Duration) (*EventRepository, error) {
	repo := &EventRepository{
		db:         db,
		logger:     logger,
		bucketSize: bucketSize,
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
func (er *EventRepository) StoreWithBatch(b *pebble.Batch, msg *storagepb.StoredMessage) ([]byte, error) {
	nextID := atomic.AddUint64(&er.lastID, 1)

	fireAtMs := msg.EnqueuedAtUnixMs + msg.DelayMs
	bucket := utils.CalculateBucket(fireAtMs, er.bucketSize)
	topicHash := utils.TopicHash(msg.Topic)
	key := utils.EventKey(bucket, topicHash, nextID)

	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, nextID)
	if err := b.Set(eventsLastIDKey, idBytes, nil); err != nil {
		return nil, err
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message for topic %q: %w", msg.Topic, err)
	}

	if err := b.Set(key, data, nil); err != nil {
		return nil, err
	}

	for _, idx := range msg.GetIndexes() {
		idxBytes, err := proto.Marshal(idx)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal index to bytes: %w", err)
		}

		if err := b.Set(idxBytes, key, nil); err != nil {
			return nil, err
		}
	}

	return key, nil
}

// StoreWithBatch stores the raw msg value in bytes.
// This is used in Raft's write paths.
// It returns the generated 24-byte Pebble key for the caller to use as a delivery_tag.
func (er *EventRepository) StoreRawWithBatch(b *pebble.Batch, bucket uint64, topicHash uint64, indexes [][]byte, msg []byte) ([]byte, error) {
	nextID := atomic.AddUint64(&er.lastID, 1)

	key := utils.EventKey(bucket, topicHash, nextID)

	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, nextID)
	if err := b.Set(eventsLastIDKey, idBytes, nil); err != nil {
		return nil, err
	}

	if err := b.Set(key, msg, nil); err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		if err := b.Set(idx, key, nil); err != nil {
			return nil, err
		}
	}

	return key, nil
}

func (er *EventRepository) DeleteWithBatch(b *pebble.Batch, key []byte) error {
	return b.Delete(key, nil)
}
