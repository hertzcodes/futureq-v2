package raft

import (
	"encoding/binary"
	"fmt"
)

// CommandType identifies the state machine operation.
type CommandType uint8

const (
	StoreEventCmd CommandType = iota
)

// Command is the unit of work proposed to the Raft cluster.
//
// Wire format (9+ bytes, zero allocations on marshal):
//
//	[0]    : CommandType (1 byte)
//	[1..8] : Bucket     (8 bytes, big-endian uint64)
//	[9..]  : Data       (variable length, verbatim copy)
type Command struct {
	Type   CommandType
	Bucket uint64
	Data   []byte
}

// MarshalCommand serialises cmd into a compact binary representation.
// The resulting slice is safe to pass to Dragonboat's SyncPropose.
func MarshalCommand(cmd *Command) ([]byte, error) {
	out := make([]byte, 1+8+len(cmd.Data))
	out[0] = byte(cmd.Type)
	binary.BigEndian.PutUint64(out[1:9], cmd.Bucket)
	copy(out[9:], cmd.Data)
	return out, nil
}

// UnmarshalCommand deserialises a command previously encoded by MarshalCommand.
// The returned Data slice aliases the input slice — do not mutate data after
// calling this function if you intend to keep the Command alive.
func UnmarshalCommand(data []byte) (*Command, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("raft: command payload too short: got %d bytes, need at least 9", len(data))
	}
	return &Command{
		Type:   CommandType(data[0]),
		Bucket: binary.BigEndian.Uint64(data[1:9]),
		Data:   data[9:],
	}, nil
}
