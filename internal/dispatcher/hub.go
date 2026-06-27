package dispatcher

import (
	"fmt"
	"sync"
	"sync/atomic"

	pb "github.com/futureq-io/protocol/proto/go"
	"go.uber.org/zap"
)

// ActiveTopic describes a (topic, group) pair that has at least one connected consumer.
type ActiveTopic struct {
	Topic     string
	TopicHash uint64
	GroupID   string
}

// consumerEntry holds one consumer's state within a group.
type consumerEntry struct {
	id    string
	topic string
	group string
	ch    chan *pb.QueueMessage
}

// Hub manages consumer connections indexed by (topic, group_id).
// Within each group, messages are delivered to exactly one consumer
// (round-robin). Different groups on the same topic each get an
// independent copy of every message (fan-out).
type Hub struct {
	mu sync.RWMutex

	// groups: topic → groupID → []*consumerEntry
	groups map[string]map[string][]*consumerEntry

	// rrIndex: "topic|group" → next round-robin index (atomic)
	rrIndex sync.Map

	// byID: consumerID → *consumerEntry (fast lookup for unregister)
	byID map[string]*consumerEntry

	// inFlightByConsumer: consumerID → []keyString (keys in-flight to that consumer)
	// Protected by inFlightMu; used for bulk cleanup on disconnect.
	inFlightByConsumer map[string][]string
	inFlightMu         sync.Mutex

	logger *zap.Logger
	wakeCh chan struct{}
}

// NewHub constructs a Hub. wakeCh is signalled when a new consumer connects,
// causing the dispatcher to immediately scan for due messages.
func NewHub(logger *zap.Logger, wakeCh chan struct{}) *Hub {
	return &Hub{
		groups:             make(map[string]map[string][]*consumerEntry),
		byID:               make(map[string]*consumerEntry),
		inFlightByConsumer: make(map[string][]string),
		logger:             logger.Named("hub"),
		wakeCh:             wakeCh,
	}
}

// Register adds a consumer to the Hub under the given topic and group.
func (h *Hub) Register(id, topic, groupID string, ch chan *pb.QueueMessage) {
	e := &consumerEntry{
		id:    id,
		topic: topic,
		group: groupID,
		ch:    ch,
	}

	h.mu.Lock()
	if h.groups[topic] == nil {
		h.groups[topic] = make(map[string][]*consumerEntry)
	}
	h.groups[topic][groupID] = append(h.groups[topic][groupID], e)
	h.byID[id] = e
	h.mu.Unlock()

	h.logger.Info("consumer registered",
		zap.String("id", id),
		zap.String("topic", topic),
		zap.String("group", groupID),
	)

	// Wake the dispatcher loop immediately.
	select {
	case h.wakeCh <- struct{}{}:
	default:
	}
}

// Unregister removes a consumer from the Hub and returns the set of in-flight
// key strings that were associated with it (so the dispatcher can remove them
// from the in-flight map and re-dispatch those messages).
func (h *Hub) Unregister(id string) []string {
	h.mu.Lock()
	e, ok := h.byID[id]
	if !ok {
		h.mu.Unlock()
		return nil
	}

	// Remove from group list.
	group := h.groups[e.topic][e.group]
	for i, ce := range group {
		if ce.id == id {
			h.groups[e.topic][e.group] = append(group[:i], group[i+1:]...)
			break
		}
	}
	// Clean up empty maps.
	if len(h.groups[e.topic][e.group]) == 0 {
		delete(h.groups[e.topic], e.group)
	}
	if len(h.groups[e.topic]) == 0 {
		delete(h.groups, e.topic)
	}
	delete(h.byID, id)
	h.mu.Unlock()

	h.logger.Info("consumer unregistered",
		zap.String("id", id),
		zap.String("topic", e.topic),
		zap.String("group", e.group),
	)

	// Return in-flight keys for this consumer.
	h.inFlightMu.Lock()
	keys := h.inFlightByConsumer[id]
	delete(h.inFlightByConsumer, id)
	h.inFlightMu.Unlock()

	return keys
}

// DispatchToGroup sends msg to exactly one available consumer in (topic, groupID).
// It uses round-robin selection among the group's consumers and skips full channels.
// Returns the consumerID that received the message, or "" if no consumer was available.
func (h *Hub) DispatchToGroup(topic, groupID string, msg *pb.QueueMessage, keyStr string) string {
	h.mu.RLock()
	groups, ok := h.groups[topic]
	if !ok {
		h.mu.RUnlock()
		return ""
	}
	consumers := groups[groupID]
	if len(consumers) == 0 {
		h.mu.RUnlock()
		return ""
	}
	// Make a shallow copy to iterate safely after releasing the lock.
	snap := make([]*consumerEntry, len(consumers))
	copy(snap, consumers)
	h.mu.RUnlock()

	// Round-robin starting index.
	rrKey := fmt.Sprintf("%s|%s", topic, groupID)
	var idx uint64
	if v, loaded := h.rrIndex.Load(rrKey); loaded {
		idx = v.(uint64)
	}

	n := uint64(len(snap))
	for i := uint64(0); i < n; i++ {
		candidate := snap[(idx+i)%n]
		select {
		case candidate.ch <- msg:
			// Advance round-robin counter.
			h.rrIndex.Store(rrKey, (idx+i+1)%n)
			// Track in-flight key for this consumer.
			h.inFlightMu.Lock()
			h.inFlightByConsumer[candidate.id] = append(h.inFlightByConsumer[candidate.id], keyStr)
			h.inFlightMu.Unlock()
			return candidate.id
		default:
			h.logger.Warn("consumer channel full, skipping",
				zap.String("consumer_id", candidate.id),
				zap.String("topic", topic),
				zap.String("group", groupID),
			)
		}
	}

	return ""
}

// RemoveInFlightForConsumer removes a specific key from a consumer's in-flight
// tracking. Called when the consumer ACKs or NACKs a message.
func (h *Hub) RemoveInFlightForConsumer(consumerID, keyStr string) {
	h.inFlightMu.Lock()
	defer h.inFlightMu.Unlock()
	keys := h.inFlightByConsumer[consumerID]
	for i, k := range keys {
		if k == keyStr {
			h.inFlightByConsumer[consumerID] = append(keys[:i], keys[i+1:]...)
			return
		}
	}
}

// ActiveTopics returns a snapshot of all (topic, topicHash, groupID) tuples
// that currently have at least one connected consumer. The dispatcher uses
// this to scope its Pebble scan.
func (h *Hub) ActiveTopics() []ActiveTopic {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var result []ActiveTopic
	for topic, groups := range h.groups {
		for groupID, consumers := range groups {
			if len(consumers) > 0 {
				// Import xxhash at call site to avoid circular imports.
				// TopicHash is computed by the caller via utils.TopicHash.
				result = append(result, ActiveTopic{
					Topic:   topic,
					GroupID: groupID,
				})
			}
		}
	}
	return result
}

// HasConsumers returns true if at least one consumer is currently connected.
func (h *Hub) HasConsumers() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.byID) > 0
}

// GroupsForTopic returns a snapshot of all group IDs that have active consumers
// for the given topic.
func (h *Hub) GroupsForTopic(topic string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	groups, ok := h.groups[topic]
	if !ok {
		return nil
	}
	result := make([]string, 0, len(groups))
	for gid, consumers := range groups {
		if len(consumers) > 0 {
			result = append(result, gid)
		}
	}
	return result
}

// atomicUint64 is a helper for atomic operations via sync/atomic.
var _ = atomic.AddUint64
