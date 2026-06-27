package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	// Producer metrics
	MessagesPublishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "futureq_messages_published_total",
		Help: "Total number of messages successfully published.",
	}, []string{"topic", "ack_level"})

	PublishBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "futureq_publish_batch_size",
		Help:    "Distribution of batch sizes for PublishStream RPCs.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1..4096
	}, []string{"topic"})

	RaftProposeDurationMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "futureq_raft_propose_duration_ms",
		Help:    "Latency of Raft SyncPropose calls in milliseconds.",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 16), // 0.5ms..16s
	}, []string{"ack_level"})

	// Consumer metrics
	MessagesDispatchedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "futureq_messages_dispatched_total",
		Help: "Total number of messages dispatched to consumers.",
	}, []string{"topic", "group_id"})

	MessagesExpiredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "futureq_messages_expired_total",
		Help: "Total number of messages discarded due to TTL expiry.",
	}, []string{"topic"})

	MessagesInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "futureq_messages_in_flight",
		Help: "Current number of dispatched but unacknowledged messages.",
	}, []string{"topic", "group_id"})

	ConsumerAckTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "futureq_consumer_ack_total",
		Help: "Total number of consumer acknowledgements received.",
	}, []string{"topic", "group_id", "success"})

	ActiveConsumers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "futureq_active_consumers",
		Help: "Current number of connected consumers.",
	}, []string{"topic", "group_id"})

	// Dispatcher metrics
	DispatcherPassDurationMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "futureq_dispatcher_pass_duration_ms",
		Help:    "Duration of each dispatcher scan pass in milliseconds.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 16),
	})

	DeleteBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "futureq_delete_batch_size",
		Help:    "Distribution of deletion batch sizes.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})

	// Raft metrics
	RaftLeaderChangesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "futureq_raft_leader_changes_total",
		Help: "Total number of Raft leader elections observed by this node.",
	})

	RaftReplicationLagEntries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "futureq_raft_replication_lag_entries",
		Help: "Number of log entries this node is behind the leader.",
	}, []string{"node_id"})
)

// Server wraps the Prometheus HTTP metrics server.
type Server struct {
	addr   string
	logger *zap.Logger
}

// NewServer creates a metrics HTTP server that will expose /metrics.
// addr should be in the form "host:port" (e.g. "0.0.0.0:9090").
func NewServer(addr string, logger *zap.Logger) *Server {
	return &Server{
		addr:   addr,
		logger: logger.Named("metrics"),
	}
}

// Run starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) {
	if s.addr == "" {
		s.logger.Info("metrics server disabled (no listen address configured)")
		<-ctx.Done()
		return
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	go func() {
		s.logger.Info("metrics server listening", zap.String("address", s.addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("metrics server error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	s.logger.Info("metrics server: shutting down")
	_ = srv.Shutdown(context.Background()) //nolint:contextcheck
}
