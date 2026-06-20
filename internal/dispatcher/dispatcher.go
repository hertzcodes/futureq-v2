package dispatcher

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/pkg/utils"
	pb "github.com/futureq-io/futureq/proto/go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Dispatcher struct {
	db       *pebble.DB
	hub      *Hub
	logger   *zap.Logger
	interval time.Duration
	wakeCh   chan struct{}
	inFlight sync.Map
}

func NewDispatcher(db *pebble.DB, hub *Hub, interval time.Duration, wakeCh chan struct{}, logger *zap.Logger) *Dispatcher {
	return &Dispatcher{
		db:       db,
		hub:      hub,
		logger:   logger.Named("dispatcher"),
		interval: interval,
		wakeCh:   wakeCh,
	}
}

// RemoveInFlight removes a message from the in-flight tracker, allowing it to be dispatched again if it still exists.
func (d *Dispatcher) RemoveInFlight(key []byte) {
	d.inFlight.Delete(string(key))
}

func (d *Dispatcher) Run(ctx context.Context) {
	timer := time.NewTimer(d.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.wakeCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			d.doPass()
			timer.Reset(d.interval)
		case <-timer.C:
			dispatched := d.doPass()
			if dispatched > 0 {
				timer.Reset(0)
			} else {
				timer.Reset(d.interval)
			}
		}
	}
}

func (d *Dispatcher) doPass() int {
	if !d.hub.HasConsumers() {
		return 0
	}

	if app.A.NodeHost != nil {
		shardID := app.A.Config().Raft.ClusterID
		leaderID, _, valid, err := app.A.NodeHost.GetLeaderID(shardID)
		if err != nil || !valid || leaderID != app.A.Config().Raft.NodeID {
			return 0
		}
	}

	nowBucket := utils.CalculateBucket(time.Now().UnixMilli(), app.A.Config().Storage.TimeBucketSize)

	upperBound := make([]byte, 16)
	binary.BigEndian.PutUint64(upperBound, nowBucket+1)

	iter, err := d.db.NewIter(&pebble.IterOptions{
		UpperBound: upperBound,
	})
	if err != nil {
		d.logger.Error("failed to create iterator", zap.Error(err))
		return 0
	}
	defer iter.Close()

	dispatched := 0

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) != 16 {
			continue
		}

		keyStr := string(key)
		if dispatchedAt, ok := d.inFlight.Load(keyStr); ok {
			// If it has been in flight for > 5 seconds, assume consumer crashed and re-dispatch it
			if time.Since(dispatchedAt.(time.Time)) < 5*time.Second {
				continue
			}
		}

		val := iter.Value()

		var req pb.StreamPublishRequest
		if err := proto.Unmarshal(val, &req); err != nil {
			d.logger.Error("failed to unmarshal stored event", zap.Error(err))
			continue
		}

		// Make a copy of the key because Pebble reuses iterator buffers,
		// and we are passing this key to consumer channels and subsequently the Deleter.
		keyCopy := make([]byte, 16)
		copy(keyCopy, key)

		msg := &pb.QueueMessage{
			Payload:     req.Payload,
			DeliveryTag: keyCopy,
		}

		sent := d.hub.Broadcast(msg)
		if sent == 0 {
			break
		}

		d.inFlight.Store(keyStr, time.Now())
		dispatched++
	}

	return dispatched
}
