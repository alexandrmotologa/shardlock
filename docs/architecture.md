# Architecture

ShardLock coordinates distributed leases by combining the Raft consensus algorithm with a state machine that tracks active locks and monotonically increasing fencing tokens.

## System boundaries

The application uses hexagonal architecture to isolate the core consensus engine from network protocols and disk operations:

```
+-------------------------------------------------------------------+
|                        Infrastructure Layer                       |
|                                                                   |
|   +-----------------------+               +-------------------+   |
|   |  Embedded HTTP & SSE  |               |    CLI Runner     |   |
|   +-----------+-----------+               +---------+---------+   |
|               |                                     |             |
|               v                                     v             |
|   +-----------------------------------------------------------+   |
|   |                    Application Layer                      |   |
|   |  - RaftNodeService (Consensus orchestration)              |   |
|   |  - LockCoordinator (Linearizable lock operations)         |   |
|   |  - SessionTracker  (Client heartbeat tracking)            |   |
|   +---------------------------+-------------------------------+   |
|                               |                                   |
|                               v                                   |
|   +-----------------------------------------------------------+   |
|   |                      Domain Layer                         |   |
|   |  - RaftRole (Leader, Follower, Candidate)                 |   |
|   |  - RaftLog & LogEntry                                     |   |
|   |  - RaftStateMachine (Locks & Monotonic Counter)           |   |
|   |  - Ports (StoragePort, TransportPort)                     |   |
|   +---------------------------+-------------------------------+   |
|                               |                                   |
|               +---------------+---------------+                   |
|               |                               |                   |
|               v                               v                   |
|   +-----------------------+       +---------------------------+   |
|   |  Async TCP Transport  |       |   Disk WAL & Snapshot     |   |
|   |  (P2P Raft Channels)  |       |   (FileChannel & CRC32)   |   |
|   +-----------------------+       +---------------------------+   |
+-------------------------------------------------------------------+
```

## Layers

### 1. Domain layer

The domain package contains the consensus state machine and business entities. It has no dependencies outside the standard Java runtime library (`java.*`).

Key components:
- `NodeRole`: Enum representing `LEADER`, `FOLLOWER`, or `CANDIDATE`.
- `Term`: Value object representing the Raft election epoch.
- `RaftLog`: Ordered sequence of log entries with term tracking, uncommitted entry truncation, and commit index markers.
- `RaftStateMachine`: In-memory index of active locks, lease expiration timestamps, client sessions, and the global monotonic fencing counter.
- `StoragePort`: Outbound interface for log entry persistence and state machine snapshots.
- `TransportPort`: Outbound interface for peer-to-peer RPC transmission (`RequestVote` and `AppendEntries`).

### 2. Application layer

The application layer coordinates domain logic with external interactions:
- `RaftNodeService`: Manages election timers, candidate step-up, vote collection, log replication, and heartbeat distribution.
- `LockCoordinator`: Handles client lock acquisition, renewal, and explicit release. Only the active leader processes lock requests; follower nodes return a redirect header containing the known leader address.
- `SessionTracker`: Runs a virtual thread timer that detects expired leases and appends an automatic unlock record to the log.

### 3. Infrastructure layer

The infrastructure layer adapts domain ports to external systems:
- `FileChannelWalStorage`: Appends log entries to disk using length-delimited binary frames with CRC32 checksums.
- `AsyncTcpTransport`: Uses Java 21 non-blocking sockets and virtual threads for peer-to-peer RPC communication.
- `EmbeddedHttpServer`: Serves client REST endpoints and SSE streams using Java standard library `com.sun.net.httpserver.HttpServer`.

## Threading model

ShardLock uses Java 21 Virtual Threads.
- Each incoming TCP connection runs on an independent virtual thread.
- Each HTTP client request runs on a virtual thread.
- Scheduled heartbeat checks and election timeout monitors run on lightweight virtual thread executors, avoiding thread pool starvation and complex thread pool sizing.
