# FutureQ

FutureQ is a high-performance, distributed delayed-message queue broker written in Go. It allows producers to publish messages with a relative delay, and ensures they are dispatched to consumers when their delay expires. 

Built with strong consistency, durability, and high availability in mind, FutureQ leverages a powerful embedded storage engine and a robust Raft consensus implementation to provide a reliable messaging backbone for modern distributed systems.

## Key Features

*   **Delayed Messaging**: Enqueue messages to be delivered after a specific `delay_ms`.
*   **Durable Storage**: Uses [Pebble](https://github.com/cockroachdb/pebble) (CockroachDB's embedded LSM key-value store) for extremely fast and reliable disk-backed storage.
*   **High Availability & Replication**: Employs [Dragonboat](https://github.com/lni/dragonboat) for Raft consensus, ensuring data is replicated and safe across cluster nodes.
*   **High Throughput**: Supports batch publishing and batch acknowledgements to minimize Raft and storage overhead.
*   **Consumer Groups & Topics**: Supports topic-based routing with fan-out across multiple consumer groups, and competing consumers (round-robin) within a single group.
*   **gRPC Transport**: Uses efficient bidirectional gRPC streams for both producing and consuming messages.
*   **Automatic Cluster Membership**: Uses HashiCorp's `memberlist` (gossip protocol) for automatic node discovery and cluster scaling.
*   **Message Expiry (TTL)**: Native support for message Time-To-Live, automatically cleaning up expired messages that haven't been consumed.
*   **Observability**: Exposes Prometheus metrics for deep visibility into queue performance, Raft latency, and consumer lag.

## Architecture

FutureQ operates as a cluster of nodes where one node acts as the Raft Leader, and others as Followers.
Producers and consumers connect via gRPC. 

### Core Components:

*   **Storage (`internal/storage`)**: Interfaces with Pebble DB. The key schema is heavily optimized for time-based range scans: `[bucket][topic_hash][event_id]`.
*   **Consensus (`internal/raft`)**: Defines the Raft State Machine and handles replicated commands (like `StoreBatchCmd` and `DeleteBatchCmd`).
*   **Dispatcher (`internal/dispatcher`)**: The heart of the broker. It continuously scans the time buckets in Pebble for messages whose delay has expired, tracking active topics via the Hub.
*   **Hub (`internal/dispatcher/hub.go`)**: Manages connected consumers, mapping them by `(topic, group_id)`, and handles round-robin message delivery.
*   **API (`internal/api`)**: gRPC services (`FutureQProducer`, `FutureQConsumer`, `FutureQCluster`).

## Project Structure

```text
.
├── cmd/                # Application entrypoints (start, root commands)
├── config/             # Configuration loading and default values (YAML/Env)
├── internal/
│   ├── api/            # gRPC service handlers (producer, consumer, cluster)
│   ├── app/            # Application lifecycle management
│   ├── dispatcher/     # Message dispatching, consumer hub, janitor, and deleter
│   ├── membership/     # Gossip protocol integration (memberlist)
│   ├── metrics/        # Prometheus metrics collection
│   ├── raft/           # Dragonboat Raft state machine and commands
│   └── repository/     # Key schema and database interaction logic
├── pkg/                # Reusable utilities (logging, xxhash wrapper, key encoding)
├── config.example.yaml # Example configuration file with detailed comments
└── plann.md            # Redesign plan and architectural decisions
```

## Getting Started

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/futureq-io/futureq.git
    cd futureq
    ```

2.  **Configuration**:
    Copy `config.example.yaml` to `config.yaml` and adjust settings as needed (e.g., node ID, listen addresses).

3.  **Run the Server**:
    ```bash
    go run cmd/main.go start --config config.yaml
    ```

## Roadmap

*   **Sharding**: Support for multiple Raft shards across a large cluster.
*   **Follower Reads**: Allowing consumers to read from replica nodes to reduce load on the leader.
*   **Security**: Implement mTLS for gRPC communication and token-based ACLs for topics.
*   **Dead-Letter Queues (DLQ)**: Automatic routing of messages that fail processing multiple times.
