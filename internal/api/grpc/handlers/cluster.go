package handlers

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/membership"
	pb "github.com/futureq-io/protocol/proto/go"
)

// ClusterHandler implements pb.FutureQClusterServer.
// Any node can respond to GetClusterInfo — it does not need to be the leader.
// JoinCluster and LeaveCluster require leader-forwarding which is handled internally.
type ClusterHandler struct {
	pb.UnimplementedFutureQClusterServer
	logger    *zap.Logger
	gossip    *membership.Manager
}

// NewClusterHandler returns an initialised ClusterHandler.
// gossip may be nil in single-node mode (no gossip started).
func NewClusterHandler(logger *zap.Logger, gossip *membership.Manager) *ClusterHandler {
	return &ClusterHandler{
		logger: logger.Named("cluster"),
		gossip: gossip,
	}
}

// GetClusterInfo returns the current cluster topology.
// Any node may respond to this RPC — clients use it to discover the current leader.
func (h *ClusterHandler) GetClusterInfo(ctx context.Context, req *pb.ClusterInfoRequest) (*pb.ClusterInfoResponse, error) {
	resp := &pb.ClusterInfoResponse{}

	if app.A.NodeHost == nil {
		// Single-node mode: this node is always the leader.
		resp.LeaderNodeId = app.A.Config().Raft.NodeID
		resp.LeaderAddress = app.A.Config().Server.Listen
		resp.Nodes = []*pb.NodeInfo{
			{
				NodeId:   app.A.Config().Raft.NodeID,
				Address:  app.A.Config().Server.Listen,
				IsLeader: true,
				IsAlive:  true,
			},
		}
		return resp, nil
	}

	// Raft mode: query Dragonboat for the current leader.
	shardID := app.A.Config().Raft.ClusterID
	leaderID, _, valid, err := app.A.NodeHost.GetLeaderID(shardID)
	if err != nil || !valid {
		leaderID = 0
	}
	resp.LeaderNodeId = leaderID

	// Build the node list from gossip membership (if available).
	if h.gossip != nil {
		members := h.gossip.Members()
		for _, m := range members {
			node := &pb.NodeInfo{
				NodeId:   m.NodeID,
				Address:  m.GRPCAddress,
				IsLeader: m.NodeID == leaderID,
				IsAlive:  m.IsAlive,
			}
			resp.Nodes = append(resp.Nodes, node)
			if node.IsLeader {
				resp.LeaderAddress = m.GRPCAddress
			}
		}
	} else {
		// No gossip: return only this node.
		isLeader := leaderID == app.A.Config().Raft.NodeID
		resp.Nodes = []*pb.NodeInfo{
			{
				NodeId:   app.A.Config().Raft.NodeID,
				Address:  app.A.Config().Server.Listen,
				IsLeader: isLeader,
				IsAlive:  true,
			},
		}
		if isLeader {
			resp.LeaderAddress = app.A.Config().Server.Listen
		}
	}

	return resp, nil
}

// JoinCluster adds a new node to the Raft cluster.
// If this node is not the leader, it returns an error telling the client to
// retry on the leader. The client should first call GetClusterInfo to find
// the leader's address.
func (h *ClusterHandler) JoinCluster(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if req.NodeId == 0 {
		return &pb.JoinResponse{Success: false, ErrorMessage: "node_id must not be zero"}, nil
	}
	if req.RaftAddress == "" {
		return &pb.JoinResponse{Success: false, ErrorMessage: "raft_address must not be empty"}, nil
	}

	if app.A.NodeHost == nil {
		return &pb.JoinResponse{Success: false, ErrorMessage: "raft is not enabled on this node"}, nil
	}

	shardID := app.A.Config().Raft.ClusterID
	leaderID, _, valid, err := app.A.NodeHost.GetLeaderID(shardID)
	if err != nil || !valid || leaderID != app.A.Config().Raft.NodeID {
		return &pb.JoinResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("not the leader; forward this request to node %d", leaderID),
		}, nil
	}

	// Request Dragonboat to add the new replica.

	if _ , err := app.A.NodeHost.RequestAddReplica(shardID, req.NodeId, req.RaftAddress, 0, 10 * time.Second); err != nil {
		h.logger.Error("failed to add replica",
			zap.Uint64("node_id", req.NodeId),
			zap.String("raft_address", req.RaftAddress),
			zap.Error(err),
		)
		return &pb.JoinResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	h.logger.Info("replica added to cluster",
		zap.Uint64("node_id", req.NodeId),
		zap.String("raft_address", req.RaftAddress),
		zap.String("grpc_address", req.GrpcAddress),
	)

	return &pb.JoinResponse{Success: true}, nil
}

// LeaveCluster removes a node from the Raft cluster gracefully.
// The departing node calls this on itself (or an operator calls it remotely).
func (h *ClusterHandler) LeaveCluster(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	if req.NodeId == 0 {
		return &pb.LeaveResponse{Success: false, ErrorMessage: "node_id must not be zero"}, nil
	}

	if app.A.NodeHost == nil {
		return &pb.LeaveResponse{Success: false, ErrorMessage: "raft is not enabled on this node"}, nil
	}

	shardID := app.A.Config().Raft.ClusterID
	leaderID, _, valid, err := app.A.NodeHost.GetLeaderID(shardID)
	if err != nil || !valid || leaderID != app.A.Config().Raft.NodeID {
		return &pb.LeaveResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("not the leader; forward this request to node %d", leaderID),
		}, nil
	}

	if _ , err := app.A.NodeHost.RequestDeleteReplica(shardID, req.NodeId, 0, 10 * time.Second); err != nil {
		h.logger.Error("failed to remove replica",
			zap.Uint64("node_id", req.NodeId),
			zap.Error(err),
		)
		return &pb.LeaveResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	h.logger.Info("replica removed from cluster", zap.Uint64("node_id", req.NodeId))

	return &pb.LeaveResponse{Success: true}, nil
}
