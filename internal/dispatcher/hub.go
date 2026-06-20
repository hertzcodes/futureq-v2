package dispatcher

import (
	"sync"

	pb "github.com/futureq-io/futureq/proto/go"
	"go.uber.org/zap"
)

type Hub struct {
	mu        sync.RWMutex
	consumers map[string]chan *pb.QueueMessage
	logger    *zap.Logger
	wakeCh    chan struct{}
}

func NewHub(logger *zap.Logger, wakeCh chan struct{}) *Hub {
	return &Hub{
		consumers: make(map[string]chan *pb.QueueMessage),
		logger:    logger.Named("hub"),
		wakeCh:    wakeCh,
	}
}

func (h *Hub) Register(id string, ch chan *pb.QueueMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consumers[id] = ch
	h.logger.Debug("consumer registered", zap.String("id", id))

	// Wake the dispatcher loop so it immediately scans for new messages
	// instead of waiting for the poll interval to elapse.
	select {
	case h.wakeCh <- struct{}{}:
	default:
	}
}

func (h *Hub) Unregister(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.consumers, id)
	h.logger.Debug("consumer unregistered", zap.String("id", id))
}

func (h *Hub) Broadcast(msg *pb.QueueMessage) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	sentCount := 0
	for id, ch := range h.consumers {
		select {
		case ch <- msg:
			sentCount++
		default:
			h.logger.Warn("consumer channel full, dropping message", zap.String("id", id), zap.String("delivery_tag", string(msg.GetDeliveryTag())))
		}
	}
	return sentCount
}

func (h *Hub) HasConsumers() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.consumers) > 0
}
