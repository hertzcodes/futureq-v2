package raft_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"sync/atomic"

	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/config"
	"github.com/futureq-io/futureq/internal/raft"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var portBase atomic.Uint32

func init() {
	portBase.Store(50005)
}

func TestRaftReplicationWithWALDisabled(t *testing.T) {
	testReplication(t, true)
}

func TestRaftReplicationWithWALEnabled(t *testing.T) {
	testReplication(t, false)
}

func testReplication(t *testing.T, disableWAL bool) {
	tmpdir, err := os.MkdirTemp("", "raft-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	logger := zap.NewNop()

	var apps []*app.App

	base := portBase.Add(100)

	for i := 1; i <= 3; i++ {
		cfg := config.Config{
			Server: config.Server{
				Listen: fmt.Sprintf("0.0.0.0:%d", int(base)+10+i),
			},
			Storage: config.Storage{
				Persist: true,
				Pebble: config.Pebble{
					DisableWAL:       disableWAL,
					DataPath:         fmt.Sprintf("%s/pebble-%d", tmpdir, i),
					CacheSizeMB:      1,
					InMemTableSizeMB: 1,
				},
			},
			Raft: config.Raft{
				NodeID:        uint64(i),
				ClusterID:     1,
				ListenAddress: fmt.Sprintf("0.0.0.0:%d", int(base)+i),
				DataPath:      fmt.Sprintf("%s/raft-%d", tmpdir, i),
				InitialMembers: map[uint64]string{
					1: fmt.Sprintf("0.0.0.0:%d", int(base)+1),
					2: fmt.Sprintf("0.0.0.0:%d", int(base)+2),
					3: fmt.Sprintf("0.0.0.0:%d", int(base)+3),
				},
			},
		}

		a, err := app.Init(&cfg, logger)
		require.NoError(t, err)
		apps = append(apps, a)
	}

	defer func() {
		for _, a := range apps {
			if a.NodeHost != nil {
				a.NodeHost.Close()
			}
			if a.Pebble != nil && a.Pebble.DB != nil {
				_ = a.Pebble.DB.Close()
			}
		}
	}()

	// Wait for election
	var leaderApp *app.App
	fmt.Println("Waiting for election...")
	require.Eventually(t, func() bool {
		for _, a := range apps {
			leaderID, _, valid, _ := a.NodeHost.GetLeaderID(1)
			if valid && leaderID == a.Config().Raft.NodeID {
				leaderApp = a
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "should elect a leader")
	fmt.Println("Elected leader!")

	// Propose a message
	cmd := &raft.Command{
		Type:   raft.StoreEventCmd,
		Bucket: 100,
		Data:   []byte("test_data"),
	}
	cmdBytes, err := raft.MarshalCommand(cmd)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	fmt.Println("Proposing command...")
	session := leaderApp.NodeHost.GetNoOPSession(1)
	res, err := leaderApp.NodeHost.SyncPropose(ctx, session, cmdBytes)
	require.NoError(t, err)
	require.Equal(t, uint64(1), res.Value)
	fmt.Println("Command proposed successfully!")

	// Check if data is replicated on ALL nodes
	for i, a := range apps {
		fmt.Printf("Checking follower %d\n", i+1)
		require.Eventuallyf(t, func() bool {
			iter := a.Pebble.DB.NewIter(nil)
			defer iter.Close()
			for iter.First(); iter.Valid(); iter.Next() {
				if string(iter.Value()) == "test_data" {
					return true
				}
			}
			return false
		}, 5*time.Second, 100*time.Millisecond, "follower %d should have the replicated data", i+1)
		fmt.Printf("Follower %d has the data!\n", i+1)
	}
}
