package membership

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

// NodeMeta is the structured metadata broadcast by each node over gossip.
// It is serialised as JSON into the memberlist node.Meta field.
type NodeMeta struct {
	NodeID      uint64 `json:"nodeId"`
	GRPCAddress string `json:"grpcAddress"`
	RaftAddress string `json:"raftAddress"`
}

// MemberInfo is the in-process view of a single cluster member.
type MemberInfo struct {
	Name        string
	Addr        net.IP
	Port        uint16
	NodeID      uint64
	GRPCAddress string
	RaftAddress string
	IsAlive     bool
}

// Manager manages the gossip-based cluster membership layer using
// hashicorp/memberlist. It broadcasts this node's metadata and maintains a
// live view of all peer nodes.
//
// Note: This is the cluster membership/topology layer only. Raft consensus is
// handled separately by Dragonboat. The two are coordinated by the cluster
// handler (internal/api/grpc/handlers/cluster.go) which uses gossip to detect
// new peers and then calls Dragonboat's RequestAddReplica.
//
// Future: A future client SDK may join as a Raft observer (non-voter) to
// receive live topology updates without polling the GetClusterInfo RPC. This
// design leaves that path open by keeping the NodeMeta extensible.
type Manager struct {
	list   *memberlist.Memberlist
	meta   NodeMeta
	mu     sync.RWMutex
	logger *zap.Logger

	// OnJoin is called when a new peer joins the gossip cluster.
	// The caller (cluster handler) can use this to add the peer to Raft.
	OnJoin func(meta NodeMeta)

	// OnLeave is called when a peer leaves or is detected as dead.
	OnLeave func(meta NodeMeta)
}

// Config holds the parameters needed to initialise the gossip manager.
type Config struct {
	// NodeID is this node's unique Raft node ID.
	NodeID uint64

	// BindAddress is the gossip listen address ("host:port").
	BindAddress string

	// GRPCAddress is the gRPC listen address broadcast to peers.
	GRPCAddress string

	// RaftAddress is the Raft (Dragonboat) listen address broadcast to peers.
	RaftAddress string

	// JoinPeers is a list of existing peer gossip addresses to contact on startup.
	// Leave empty for a fresh single-node bootstrap.
	JoinPeers []string
}

// NewManager creates and starts the gossip membership manager.
// It returns an error if the memberlist cannot be created or joined.
func NewManager(cfg Config, logger *zap.Logger) (*Manager, error) {
	m := &Manager{
		meta: NodeMeta{
			NodeID:      cfg.NodeID,
			GRPCAddress: cfg.GRPCAddress,
			RaftAddress: cfg.RaftAddress,
		},
		logger: logger.Named("membership"),
	}

	host, portStr, err := net.SplitHostPort(cfg.BindAddress)
	if err != nil {
		return nil, fmt.Errorf("membership: invalid bind address %q: %w", cfg.BindAddress, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("membership: invalid port in bind address %q: %w", cfg.BindAddress, err)
	}

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.BindAddr = host
	mlCfg.BindPort = port
	mlCfg.AdvertisePort = port
	mlCfg.Name = fmt.Sprintf("node-%d", cfg.NodeID)
	mlCfg.Events = &eventDelegate{manager: m}
	mlCfg.Delegate = &metaDelegate{manager: m}

	list, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("membership: failed to create memberlist: %w", err)
	}
	m.list = list

	// Join existing peers if provided.
	if len(cfg.JoinPeers) > 0 {
		n, err := list.Join(cfg.JoinPeers)
		if err != nil {
			logger.Warn("membership: could not join all peers",
				zap.Strings("peers", cfg.JoinPeers),
				zap.Error(err),
			)
		} else {
			logger.Info("membership: joined cluster", zap.Int("peers_contacted", n))
		}
	}

	logger.Info("membership: gossip started",
		zap.String("bind", cfg.BindAddress),
		zap.Uint64("node_id", cfg.NodeID),
	)

	return m, nil
}

// Members returns a snapshot of all currently known live cluster members.
func (m *Manager) Members() []MemberInfo {
	members := m.list.Members()
	result := make([]MemberInfo, 0, len(members))
	for _, node := range members {
		meta := parseNodeMeta(node.Meta)
		result = append(result, MemberInfo{
			Name:        node.Name,
			Addr:        node.Addr,
			Port:        node.Port,
			NodeID:      meta.NodeID,
			GRPCAddress: meta.GRPCAddress,
			RaftAddress: meta.RaftAddress,
			IsAlive:     node.State == memberlist.StateAlive,
		})
	}
	return result
}

// Leave gracefully departs the gossip cluster. Call before shutting down.
func (m *Manager) Leave(ctx context.Context) error {
	return m.list.Leave(0)
}

// Shutdown stops the gossip engine immediately (without a graceful leave).
func (m *Manager) Shutdown() error {
	return m.list.Shutdown()
}

// ─── internal helpers ────────────────────────────────────────────────────────

func parseNodeMeta(raw []byte) NodeMeta {
	var meta NodeMeta
	_ = json.Unmarshal(raw, &meta)
	return meta
}

// metaDelegate provides this node's metadata to memberlist.
type metaDelegate struct {
	manager *Manager
}

func (d *metaDelegate) NodeMeta(limit int) []byte {
	b, _ := json.Marshal(d.manager.meta)
	if len(b) > limit {
		return b[:limit]
	}
	return b
}

func (d *metaDelegate) NotifyMsg([]byte)                           {}
func (d *metaDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *metaDelegate) LocalState(join bool) []byte                { return nil }
func (d *metaDelegate) MergeRemoteState(buf []byte, join bool)     {}

// eventDelegate receives join/leave/update events from memberlist.
type eventDelegate struct {
	manager *Manager
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	meta := parseNodeMeta(node.Meta)
	e.manager.logger.Info("membership: node joined",
		zap.String("name", node.Name),
		zap.Uint64("node_id", meta.NodeID),
		zap.String("grpc", meta.GRPCAddress),
	)
	if e.manager.OnJoin != nil && meta.NodeID != 0 {
		e.manager.OnJoin(meta)
	}
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	meta := parseNodeMeta(node.Meta)
	e.manager.logger.Info("membership: node left",
		zap.String("name", node.Name),
		zap.Uint64("node_id", meta.NodeID),
	)
	if e.manager.OnLeave != nil && meta.NodeID != 0 {
		e.manager.OnLeave(meta)
	}
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {}