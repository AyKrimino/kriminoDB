# kriminoDB — A Simple Distributed Key-Value Store

kriminoDB is an in-memory key-value store built in Go with distributed replication capabilities. Inspired by Redis, it provides a TCP interface for client operations while using a gossip protocol for peer-to-peer communication and data synchronization.

## Current Status

- **Multi-node clustering** with automatic peer discovery
- **Gossip-based replication** for data consistency
- **Failure detection** with automatic node removal
- **Versioned values** for conflict resolution
- **Graceful shutdown** handling
- **Memory management** with TTL for message deduplication

## Quick Start

### Prerequisites

- Go 1.20+

### Run a Single Node

```bash
make run
```

The server will listen on `localhost:3000` for client connections and `localhost:4000` for peer communication.

### Run a Cluster

Open four separate terminals:

```bash
# Terminal 1 - Node 1 (bootstrap node)
make node1

# Terminal 2 - Node 2
make node2

# Terminal 3 - Node 3
make node3

# Terminal 4 - Node 4
make node4
```

Node 1 acts as the bootstrap node. Nodes 2-4 will automatically join the cluster and synchronize data.

### Test with telnet

Connect to any node's client port:

```bash
telnet localhost 3000
```

Commands:
```
SET key value
GET key
```

Example:
```
SET name "Ayoub Krimi"
GET name
```

## Architecture Overview

kriminoDB consists of three main components:

1. **Store**: In-memory key-value database with versioned values for conflict resolution
2. **Server**: TCP interface for client operations (SET/GET commands)
3. **Gossip**: Peer-to-peer protocol for:
   - Cluster membership management
   - Data replication
   - Failure detection
   - Anti-entropy repair

The gossip protocol runs every 2 seconds, propagating updates and checking peer liveness. Nodes automatically detect and remove unresponsive peers after 20 seconds of no contact.

## Command Reference

| Command | Usage               | Description                  |
| ------- | ------------------- | ---------------------------- |
| `SET`   | `SET <key> <value>` | Store a key-value pair       |
| `GET`   | `GET <key>`         | Retrieve the value for a key |

Notes:
- Keys and values are treated as strings
- Values can contain spaces
- Commands are case-insensitive
- All operations are replicated to peer nodes

## Build and Run

### Build the binary
```bash
make build
```

### Run tests
```bash
make test
```

### Clean build artifacts
```bash
make clean
```

## Design Decisions

- **Gossip protocol** chosen for its simplicity and scalability in distributed systems
- **Version vectors** used for conflict resolution during replication
- **Short timeouts** (300ms) to prevent blocking operations from stalling the system
- **Background cleanup** of seen message IDs to prevent memory leaks
- **Heartbeat mechanism** combined with active liveness checks for robust failure detection

## Author

Ayoub Krimi — Computer Science Student @ ISI Ariana, Tunisia

This project demonstrates practical implementation of distributed systems concepts including replication, failure detection, and cluster management.# kriminoDB — A Simple Distributed Key-Value Store

kriminoDB is an in-memory key-value store built in Go with distributed replication capabilities. Inspired by Redis, it provides a TCP interface for client operations while using a gossip protocol for peer-to-peer communication and data synchronization.

## Current Status

- **Multi-node clustering** with automatic peer discovery
- **Gossip-based replication** for data consistency
- **Failure detection** with automatic node removal
- **Versioned values** for conflict resolution
- **Graceful shutdown** handling
- **Memory management** with TTL for message deduplication

## Quick Start

### Prerequisites

- Go 1.20+

### Run a Single Node

```bash
make run
```

The server will listen on `localhost:3000` for client connections and `localhost:4000` for peer communication.

### Run a Cluster

Open four separate terminals:

```bash
# Terminal 1 - Node 1 (bootstrap node)
make node1

# Terminal 2 - Node 2
make node2

# Terminal 3 - Node 3
make node3

# Terminal 4 - Node 4
make node4
```

Node 1 acts as the bootstrap node. Nodes 2-4 will automatically join the cluster and synchronize data.

### Test with telnet

Connect to any node's client port:

```bash
telnet localhost 3000
```

Commands:
```
SET key value
GET key
```

Example:
```
SET name "Ayoub Krimi"
GET name
```

## Architecture Overview

kriminoDB consists of three main components:

1. **Store**: In-memory key-value database with versioned values for conflict resolution
2. **Server**: TCP interface for client operations (SET/GET commands)
3. **Gossip**: Peer-to-peer protocol for:
   - Cluster membership management
   - Data replication
   - Failure detection
   - Anti-entropy repair

The gossip protocol runs every 2 seconds, propagating updates and checking peer liveness. Nodes automatically detect and remove unresponsive peers after 20 seconds of no contact.

## Command Reference

| Command | Usage               | Description                  |
| ------- | ------------------- | ---------------------------- |
| `SET`   | `SET <key> <value>` | Store a key-value pair       |
| `GET`   | `GET <key>`         | Retrieve the value for a key |

Notes:
- Keys and values are treated as strings
- Values can contain spaces
- Commands are case-insensitive
- All operations are replicated to peer nodes

## Build and Run

### Build the binary
```bash
make build
```

### Run tests
```bash
make test
```

### Clean build artifacts
```bash
make clean
```

## Design Decisions

- **Gossip protocol** chosen for its simplicity and scalability in distributed systems
- **Version vectors** used for conflict resolution during replication
- **Short timeouts** (300ms) to prevent blocking operations from stalling the system
- **Background cleanup** of seen message IDs to prevent memory leaks
- **Heartbeat mechanism** combined with active liveness checks for robust failure detection

## Author

Ayoub Krimi — Computer Science Student @ ISI Ariana, Tunisia

This project demonstrates practical implementation of distributed systems concepts including replication, failure detection, and cluster management.
