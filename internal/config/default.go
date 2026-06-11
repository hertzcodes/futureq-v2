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
}
