package repository

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

var eventsLastIDKey = []byte("metadata/event-repo/last-id")

type EventRepository struct {
	db     *pebble.DB
	lastID uint64
}

func NewEventRepository(db *pebble.DB) (*EventRepository, error) {
	repo := &EventRepository{
		db: db,
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

func (er *EventRepository) Store(bucket uint64, data []byte) error {
	nextID := atomic.AddUint64(&er.lastID, 1)

	key := eventKey(bucket, nextID)
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, nextID)

	b := er.db.NewBatch()
	defer b.Close()

	// incr last id
	_ = b.Set(eventsLastIDKey, idBytes, nil)
	// store event
	_ = b.Set(key, data, nil)

	return b.Commit(pebble.Sync)
}

func eventKey(bucket uint64, eventID uint64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key, bucket)
	binary.BigEndian.PutUint64(key[8:], eventID)
	return key
}
