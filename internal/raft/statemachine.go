package raft

import (
	"encoding/binary"
	"errors"
	"io"
	"log"

	"github.com/cockroachdb/pebble/v2"
	"github.com/futureq-io/futureq/internal/repository"
	"github.com/lni/dragonboat/v4/statemachine"
	"go.uber.org/zap"
)

var appliedIndexKey = []byte("metadata/raft/applied-index")

// EventStateMachine implements statemachine.IOnDiskStateMachine.
// Pebble is used as the durable backing store; its WAL is intentionally
// disabled in clustered mode because the Dragonboat Raft log acts as the
// authoritative write-ahead log.  On restart, Dragonboat replays any log
// entries that were committed but not yet applied, so no data is lost.
//
// Lifecycle that Dragonboat drives:
//   Open()                – load lastApplied index from Pebble; tell Dragonboat where we are
//   Update(entries)       – apply a batch of committed log entries to Pebble (NoSync is safe
//                           because the entry is already durable in the Raft log)
//   Sync()                – called after Update batches; flushes Pebble memtable to SST files
//   Lookup(query)         – optional local read (not used yet)
//   PrepareSnapshot()     – snapshot context (we pass lastApplied)
//   SaveSnapshot()        – stream full Pebble state to the writer
//   RecoverFromSnapshot() – restore full Pebble state from the reader
//   Close()               – sync pending state; DB lifetime is owned by the App
type EventStateMachine struct {
	clusterID   uint64
	nodeID      uint64
	db          *pebble.DB
	eventRepo   *repository.EventRepository
	lastApplied uint64
}

// NewEventStateMachineFactory returns the factory function that Dragonboat
// passes (clusterID, nodeID) to when it instantiates a new replica.
func NewEventStateMachineFactory(db *pebble.DB, logger *zap.Logger) func(uint64, uint64) statemachine.IOnDiskStateMachine {
	return func(clusterID uint64, nodeID uint64) statemachine.IOnDiskStateMachine {
		repo, err := repository.NewEventRepository(db, logger)
		if err != nil {
			log.Fatalf("failed to init event repo for raft state machine: %v", err)
		}
		return &EventStateMachine{
			clusterID: clusterID,
			nodeID:    nodeID,
			db:        db,
			eventRepo: repo,
		}
	}
}

// Open loads the last applied Raft index from Pebble so Dragonboat knows
// which log entries have already been applied and does not replay them.
func (s *EventStateMachine) Open(stopc <-chan struct{}) (uint64, error) {
	val, closer, err := s.db.Get(appliedIndexKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			s.lastApplied = 0
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	s.lastApplied = binary.BigEndian.Uint64(val)
	return s.lastApplied, nil
}

// Update applies a batch of committed Raft log entries to Pebble.
//
// We use pebble.NoSync here intentionally: Dragonboat guarantees the entry
// is already durable in its own WAL before calling Update.  If the process
// crashes right after Update but before Sync(), Dragonboat will simply
// re-apply the same entries on restart via log replay.  Using NoSync avoids
// a double-fsync penalty (Raft log + Pebble WAL) on every write.
func (s *EventStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	batch := s.db.NewBatch()
	defer batch.Close()

	for i := range entries {
		cmd, err := UnmarshalCommand(entries[i].Cmd)
		if err != nil {
			entries[i].Result = statemachine.Result{Value: 0}
			continue
		}

		switch cmd.Type {
		case StoreEventCmd:
			if err := s.eventRepo.StoreWithBatch(batch, cmd.Bucket, cmd.Data); err != nil {
				entries[i].Result = statemachine.Result{Value: 0}
			} else {
				entries[i].Result = statemachine.Result{Value: 1}
			}
		default:
			entries[i].Result = statemachine.Result{Value: 0}
		}

		s.lastApplied = entries[i].Index
	}

	// Persist the applied index alongside the event data so Open() can
	// correctly report our position on the next restart.
	idxBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBytes, s.lastApplied)
	if err := batch.Set(appliedIndexKey, idxBytes, nil); err != nil {
		return nil, err
	}

	// NoSync: correctness is guaranteed by the Raft log (see comment above).
	if err := batch.Commit(pebble.NoSync); err != nil {
		return nil, err
	}

	return entries, nil
}

// Sync is called by Dragonboat after a batch of Update() calls.  We flush
// Pebble's in-memory write buffer (MemTable) to SST files on disk.  This is
// especially important when Pebble's WAL is disabled: without WAL, in-memory
// data would be lost on a crash if we never flush.  Because the Raft log
// already holds the truth, a crash before Sync() is safe — entries will be
// re-applied on restart — but flushing here reduces the re-apply work on
// restart and keeps memory usage bounded.
func (s *EventStateMachine) Sync() error {
	return s.db.Flush()
}

// Lookup supports local reads directly from the state machine.
// Not yet implemented; the gRPC producer handler reads Pebble directly.
func (s *EventStateMachine) Lookup(query interface{}) (interface{}, error) {
	return nil, nil
}

// PrepareSnapshot captures any ephemeral context needed before SaveSnapshot
// starts streaming.  We pass lastApplied for informational purposes.
func (s *EventStateMachine) PrepareSnapshot() (interface{}, error) {
	return s.lastApplied, nil
}

// SaveSnapshot streams the entire Pebble database state to w.
//
// Wire format per key-value pair:
//
//	[4 bytes little-endian] key length
//	[key length bytes]      key
//	[4 bytes little-endian] value length
//	[value length bytes]    value
//
// The snapshot includes the appliedIndexKey so that the receiver's Open()
// will report the correct applied index after RecoverFromSnapshot.
func (s *EventStateMachine) SaveSnapshot(_ interface{}, w io.Writer, stopc <-chan struct{}) error {
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()

	iter, err := snapshot.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-stopc:
			return statemachine.ErrSnapshotStopped
		default:
		}

		// Copy key and value: pebble invalidates the slices on the next
		// iterator call, and binary.Write may buffer internally.
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		v := make([]byte, len(iter.Value()))
		copy(v, iter.Value())

		if err := binary.Write(w, binary.LittleEndian, uint32(len(k))); err != nil {
			return err
		}
		if _, err := w.Write(k); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
	}
	return iter.Error()
}

// RecoverFromSnapshot restores the full Pebble database from a snapshot
// produced by SaveSnapshot.
//
// IMPORTANT: Before applying any snapshot data we wipe ALL existing Pebble
// keys.  Without this step, a follower that previously had more data than
// the snapshot would retain stale keys indefinitely, causing divergence from
// the leader.
func (s *EventStateMachine) RecoverFromSnapshot(r io.Reader, stopc <-chan struct{}) error {
	// Step 1 – delete every key currently in Pebble.
	if err := s.clearDB(stopc); err != nil {
		return err
	}

	// Step 2 – stream key-value pairs from the snapshot and write them.
	batch := s.db.NewBatch()
	defer batch.Close()

	for {
		select {
		case <-stopc:
			return statemachine.ErrSnapshotStopped
		default:
		}

		var klen uint32
		if err := binary.Read(r, binary.LittleEndian, &klen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		k := make([]byte, klen)
		if _, err := io.ReadFull(r, k); err != nil {
			return err
		}

		var vlen uint32
		if err := binary.Read(r, binary.LittleEndian, &vlen); err != nil {
			return err
		}

		v := make([]byte, vlen)
		if _, err := io.ReadFull(r, v); err != nil {
			return err
		}

		if err := batch.Set(k, v, nil); err != nil {
			return err
		}
	}

	// Sync to disk: this is a complete state replacement and must be durable.
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	// Step 3 – refresh in-memory lastApplied from the just-restored DB so
	// that subsequent Update() calls record the correct index.
	val, closer, err := s.db.Get(appliedIndexKey)
	if err == nil {
		s.lastApplied = binary.BigEndian.Uint64(val)
		closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}

	return nil
}

// clearDB iterates over all Pebble keys and deletes them in a single batch.
// Called exclusively from RecoverFromSnapshot to wipe stale state before
// installing a new snapshot.
func (s *EventStateMachine) clearDB(stopc <-chan struct{}) error {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-stopc:
			return statemachine.ErrSnapshotStopped
		default:
		}
		// Copy the key: the iterator slice is reused on the next call.
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		if err := batch.Delete(k, nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}

	return batch.Commit(pebble.Sync)
}

// Close is called by Dragonboat when it stops the replica.
// We do NOT close the pebble.DB here because its lifetime is owned by
// the App (which closes it during graceful shutdown).  We do flush any
// pending memtable data so a subsequent Open() on the same DB instance
// (e.g. in tests) sees a consistent state.
func (s *EventStateMachine) Close() error {
	return s.db.Flush()
}
