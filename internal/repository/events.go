package repository

import (
	"github.com/cockroachdb/pebble"
)

type EventRepository struct {
	db *pebble.DB
}

func NewEventRepository(db *pebble.DB) *EventRepository {
	return &EventRepository{
		db: db,
	}
}

func (er *EventRepository) Store(event []byte) error {
	return nil
}
