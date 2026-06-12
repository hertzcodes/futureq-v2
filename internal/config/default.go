package config

import "time"

var defaultConfig = Config{
	Server: Server{
		Listen:   "0.0.0.0:8443",
		MaxConns: 10,
		Timeout:  5 * time.Second,
	},

	Observability: Observability{
		Logger: Logger{
			Level: "info",
		},
	},

	Storage: Storage{
		Persist:        true,
		TimeBucketSize: 1 * time.Millisecond,
		Pebble: Pebble{
			DisableWAL:       false,
			DataPath:         "./data",
			CacheSizeMB:      16,
			InMemTableSizeMB: 64,
		},
	},

	Raft: Raft{
		NodeID:             1,
		ClusterID:          1,
		ListenAddress:      "0.0.0.0:50005",
		DataPath:           "./raft-data",
		InitialMembers:     map[uint64]string{1: "0.0.0.0:50005"},
		RTTMillisecond:     200,
		FollowerReadMode:   "strong",
		SnapshotEntries:    10000,
		CompactionOverhead: 5000,
	},
}
