package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/cockroachdb/pebble/v2"
	"github.com/futureq-io/futureq/internal/app"
	"github.com/futureq-io/futureq/internal/metrics"
	"github.com/futureq-io/futureq/internal/raft"
	"github.com/futureq-io/futureq/internal/storagepb"
	"github.com/futureq-io/futureq/pkg/utils"
	pb "github.com/futureq-io/protocol/proto/go"
)

var (
	errBatchSave = errors.New("failed to save batch")
)

// ProducerHandler implements pb.FutureQProducerServer.
type ProducerHandler struct {
	pb.UnimplementedFutureQProducerServer
	logger         *zap.Logger
	timeBucketSize time.Duration
}

// NewProducerHandler returns an initialised ProducerHandler.
// In Raft mode the handler never writes directly to Pebble; all writes go
// through SyncPropose → state machine → Pebble.
// The local eventRepo is only initialised in non-Raft (standalone) mode.
func NewProducerHandler(logger *zap.Logger) *ProducerHandler {
	bucketSize := app.A.Config().Storage.TimeBucketSize

	ph := &ProducerHandler{
		logger:         logger.Named("producer"),
		timeBucketSize: bucketSize,
	}

	return ph
}

// PublishStream handles a bidirectional stream where clients send PublishBatch
// frames and receive PublishBatchAck responses.
//
// Each batch is written atomically as a single Raft log entry (in Raft mode)
// or a single Pebble batch (standalone mode). The ack_level field controls
// whether the broker waits for quorum commit (ACK_LEVEL_QUORUM, default) or
// returns immediately after leader writes (ACK_LEVEL_LEADER).
func (ph *ProducerHandler) PublishStream(stream grpc.BidiStreamingServer[pb.PublishBatch, pb.PublishBatchAck]) error {
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			ph.logger.Error("failed to receive from stream", zap.Error(err))
			return status.Errorf(codes.Internal, "stream read error: %v", err)
		}

		ackResp, streamErr := ph.processBatch(stream.Context(), batch)
		if streamErr != nil {
			return streamErr
		}

		if err := stream.Send(ackResp); err != nil {
			ph.logger.Error("failed to send PublishBatchAck", zap.Error(err))
			return status.Errorf(codes.Internal, "failed to send ack: %v", err)
		}
	}
}

// processBatch handles one PublishBatch frame and returns the corresponding ack.
func (ph *ProducerHandler) processBatch(ctx context.Context, batch *pb.PublishBatch) (*pb.PublishBatchAck, error) {
	if len(batch.Messages) == 0 {
		return &pb.PublishBatchAck{}, nil
	}

	ackLevel := batch.AckLevel
	if ackLevel == pb.AckLevel_ACK_LEVEL_UNSPECIFIED {
		ackLevel = pb.AckLevel_ACK_LEVEL_QUORUM
	}

	nowMs := time.Now().UnixMilli()

	if app.A.NodeHost != nil {
		if err := ph.processRaftBatch(ctx, batch, nowMs, ackLevel); err != nil {
			return &pb.PublishBatchAck{Success: false}, err
		}
	} else {
		ph.processStandaloneBatch(batch, nowMs)
	}

	metrics.PublishBatchSize.WithLabelValues("").Observe(float64(len(batch.Messages)))

	return &pb.PublishBatchAck{Success: true}, nil
}

func (ph *ProducerHandler) processRaftBatch(
	ctx context.Context,
	batch *pb.PublishBatch,
	nowMs int64,
	ackLevel pb.AckLevel,
) error {
	shardID := app.A.Config().Raft.ClusterID

	// Only the leader may propose.
	leaderID, _, valid, errL := app.A.NodeHost.GetLeaderID(shardID)
	if errL != nil || !valid || leaderID != app.A.Config().Raft.NodeID {
		return errors.New("node is not the cluster leader")
	}

	// Marshal each StoredMessage once. The resulting bytes travel through the
	// Raft log and are written directly to Pebble by the state machine — no
	// second serialisation step.
	items, err := ph.buildStoreBatchItems(batch, nowMs)
	if err != nil {
		return fmt.Errorf("failed to build store batch: %w", err)
	}

	cmdBytes, err := raft.MarshalStoreBatchCmd(items)
	if err != nil {
		return fmt.Errorf("failed to marshal StoreBatchCmd: %w", err)
	}

	start := time.Now()
	var proposeErr error

	switch ackLevel {
	case pb.AckLevel_ACK_LEVEL_LEADER:
		session := app.A.NodeHost.GetNoOPSession(shardID)
		_, proposeErr = app.A.NodeHost.Propose(session, cmdBytes, 5*time.Second)

	default:
		propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		session := app.A.NodeHost.GetNoOPSession(shardID)
		_, proposeErr = app.A.NodeHost.SyncPropose(propCtx, session, cmdBytes)
		cancel()
	}

	elapsed := float64(time.Since(start).Milliseconds())
	metrics.RaftProposeDurationMs.WithLabelValues(ackLevel.String()).Observe(elapsed)

	if proposeErr != nil {
		return fmt.Errorf("failed to do raft proposal: %w", proposeErr)
	}

	return nil
}

// processStandaloneBatch writes the batch directly to Pebble (non-Raft mode).
func (ph *ProducerHandler) processStandaloneBatch(
	batch *pb.PublishBatch,
	nowMs int64,
) error {
	b := app.A.Pebble.DB.NewBatch()
	defer func() { _ = b.Close() }()

	for _, msg := range batch.Messages {
		fireAtMs := nowMs + msg.DelayMs
		bucket := utils.CalculateBucket(fireAtMs, ph.timeBucketSize)
		topicHash := utils.TopicHash(msg.Topic)

		stored := &storagepb.StoredMessage{
			Topic:            msg.Topic,
			Payload:          msg.Payload,
			EnqueuedAtUnixMs: nowMs,
			DelayMs:          msg.DelayMs,
			TtlMs:            msg.TtlMs,
		}

		data, err := proto.Marshal(stored)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}

		// TODO: we might need the handled key later.
		_, err = app.A.Repositories.Events.StoreWithBatch(b, bucket, topicHash, data)
		if err != nil {
			ph.logger.Error("failed to add message to batch",
				zap.String("topic", msg.Topic), zap.Error(err))
			return fmt.Errorf("failed to store the key in batch: %w", err)
		}
	}

	if err := b.Commit(pebble.Sync); err != nil {
		ph.logger.Error("failed to commit standalone batch", zap.Error(err))
		return errBatchSave
	}

	return nil
}

// buildStoreBatchItems builds the list of StoreBatchItems that will be embedded
// in a StoreBatchCmd Raft log entry.
//
// Each message is serialised to proto bytes exactly once here. Those bytes
// travel verbatim through the Raft log and are written directly to Pebble by
// the state machine via StoreRawWithBatch — no second serialisation step occurs.
//
// The key (bucket + topicHash) is computed here so the state machine only needs
// to atomically increment the event ID counter and concatenate the three parts.
func (ph *ProducerHandler) buildStoreBatchItems(
	batch *pb.PublishBatch,
	nowMs int64,
) ([]raft.StoreBatchItem, error) {
	items := make([]raft.StoreBatchItem, 0, len(batch.Messages))

	for _, msg := range batch.Messages {
		fireAtMs := nowMs + msg.DelayMs
		bucket := utils.CalculateBucket(fireAtMs, ph.timeBucketSize)
		topicHash := utils.TopicHash(msg.Topic)

		stored := &storagepb.StoredMessage{
			Topic:            msg.Topic,
			Payload:          msg.Payload,
			EnqueuedAtUnixMs: nowMs,
			DelayMs:          msg.DelayMs,
			TtlMs:            msg.TtlMs,
		}

		// Single proto.Marshal call per message — bytes reused directly by
		// MarshalStoreBatchCmd (one copy into the command buffer) and then by
		// StoreRawWithBatch in the state machine (written to Pebble as-is).
		data, err := proto.Marshal(stored)
		if err != nil {
			ph.logger.Error("failed to marshal StoredMessage",
				zap.String("topic", msg.GetTopic()), zap.Error(err))
			return nil, fmt.Errorf("failed to marshal message for topic %q: %w", msg.Topic, err)
		}

		items = append(items, raft.StoreBatchItem{
			Bucket:    bucket,
			TopicHash: topicHash,
			Value:     data,
		})
	}

	return items, nil
}
