package config

var defaultConfig = Config{
	Observability: Observability{
		Logger: Logger{
			Level: "info",
		},
	},

	Storage: Storage{
		Persist: true,
		Pebble: Pebble{
			DisableWAL:       false,
			DataPath:         "./data",
			CacheSizeMB:      16,
			InMemTableSizeMB: 64,
		},
	},
}
