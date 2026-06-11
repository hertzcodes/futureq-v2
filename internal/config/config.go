package config

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Server        Server        `mapstructure:"server" yaml:"server"`
	Observability Observability `mapstructure:"observability" yaml:"observability"`
	Storage       Storage       `mapstructure:"storage" yaml:"storage"`
}

type Server struct {
	Listen   string        `mapstructure:"listen" yaml:"listen"`
	MaxConns uint32        `mapstructure:"maxConns" yaml:"maxConns"`
	Timeout  time.Duration `mapstructure:"timeout" yaml:"timeout"`
}

type Observability struct {
	Logger Logger `mapstructure:"logger" yaml:"logger"`
}

type Logger struct {
	Level string `mapstructure:"level" yaml:"level"`
}

type Storage struct {
	Persist        bool          `mapstructure:"persist" yaml:"persist"`
	TimeBucketSize time.Duration `mapstructure:"timeBucketSize" yaml:"timeBucketSize"`
	Pebble         Pebble        `mapstructure:"pebble" yaml:"pebble"`
}

type Pebble struct {
	DisableWAL       bool   `mapstructure:"disableWAL" yaml:"disableWAL"`
	DataPath         string `mapstructure:"dataPath" yaml:"dataPath"`
	CacheSizeMB      int64  `mapstructure:"cacheSizeMb" yaml:"cacheSizeMb"`
	InMemTableSizeMB uint64 `mapstructure:"inMemoryTableSizeMb" yaml:"inMemoryTableSizeMb"`
}

func Load(path string) (*Config, error) {
	var c Config

	v := viper.New()
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.SetEnvPrefix("futureq")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	defaultConfigBytes, err := yaml.Marshal(defaultConfig)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling default config: %w", err)
	}

	err = v.ReadConfig(bytes.NewReader(defaultConfigBytes))
	if err != nil {
		return nil, fmt.Errorf("error reading default config: %w", err)
	}

	if path != "" {
		v.SetConfigFile(path)
		err = v.MergeInConfig()
		if err != nil {
			return nil, fmt.Errorf("error merge config: %w", err)
		}
	}

	err = v.Unmarshal(&c)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling config: %w", err)
	}

	if err := c.runPostLoadHooks(); err != nil {
		return nil, fmt.Errorf("failed to run post load hooks for config: %w", err)
	}

	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("error validating config: %w", err)
	}

	return &c, nil
}

func (c *Config) validate() error {
	if err := c.validateStorage(); err != nil {
		return err
	}

	return nil
}

func (c *Config) validateStorage() error {
	if c.Storage.Persist && c.Storage.Pebble.DataPath == "" {
		return fmt.Errorf("pebble's data path cannot be empty when persist is true")
	}

	if c.Storage.TimeBucketSize < 1*time.Millisecond {
		return fmt.Errorf("time bucket size %s is too short! Minimum amount must be 1ms", c.Storage.TimeBucketSize)
	}

	return nil
}

func (c *Config) runPostLoadHooks() error {
	if !c.Storage.Persist {
		c.Storage.Pebble.DataPath = ""
	}

	return nil
}
