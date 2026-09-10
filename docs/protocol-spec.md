# Protocol specification

ShardLock nodes communicate using two distinct protocols:
1. An asynchronous binary TCP protocol for peer-to-peer Raft consensus messages.
2. An HTTP/JSON REST API for client operations and cluster status observation.

## Peer-to-peer binary framing

All peer-to-peer TCP traffic uses length-prefixed frames:

```
+----------------+----------------+----------------+--------------------------+
| Magic (2 bytes)| Type (2 bytes) | Length (4 byte)| Payload (N bytes, JSON)  |
| 0x53 0x4C      | Message Type   | Big-Endian int | UTF-8 Encoded JSON Data  |
+----------------+----------------+----------------+--------------------------+
```

### Message types

- `0x0001`: `REQUEST_VOTE_REQUEST`
- `0x0002`: `REQUEST_VOTE_RESPONSE`
- `0x0003`: `APPEND_ENTRIES_REQUEST`
- `0x0004`: `APPEND_ENTRIES_RESPONSE`
- `0x0005`: `HEARTBEAT_PING`
- `0x0006`: `HEARTBEAT_PONG`

### RPC structures

#### RequestVote RPC

Arguments:
- `term`: Candidate's term (long)
- `candidateId`: Candidate identifier (string)
- `lastLogIndex`: Index of candidate's last log entry (long)
- `lastLogTerm`: Term of candidate's last log entry (long)

Results:
- `term`: Current term for candidate to update itself (long)
- `voteGranted`: True means candidate received vote (boolean)

#### AppendEntries RPC

Arguments:
- `term`: Leader's term (long)
- `leaderId`: Leader identifier (string)
- `prevLogIndex`: Index of log entry immediately preceding new ones (long)
- `prevLogTerm`: Term of prevLogIndex entry (long)
- `entries`: Array of log entries to store (empty for heartbeat)
- `leaderCommit`: Leader's commit index (long)

Results:
- `term`: Current term for leader to update itself (long)
- `success`: True if follower matched prevLogIndex and prevLogTerm (boolean)
- `matchIndex`: Highest log index synchronized on follower (long)

## Election rules

1. A follower resets its election timer whenever it receives an `AppendEntries` RPC or grants a vote in a `RequestVote` RPC.
2. If an election timer elapses (randomized between 150ms and 300ms), the follower transitions to candidate, increments its current term, votes for itself, and sends `RequestVote` RPCs to all peers.
3. A node grants a vote only if:
   - The candidate's term is at least as large as the node's current term.
   - The node has not voted for another candidate in this term.
   - The candidate's log is at least as up to date as the receiver's log (comparing last term, then last index).
4. When a candidate receives votes from a majority of nodes, it becomes leader and immediately sends heartbeats to all peers.

## Log replication rules

1. When a client submits a lock command to the leader, the leader appends the entry to its local log.
2. The leader issues `AppendEntries` to all followers in parallel.
3. When a majority of nodes acknowledge the entry, the leader increments its commit index, applies the command to its state machine, and replies to the client.
4. If a follower's log conflicts with the leader's log, the follower truncates the conflicting entry and overwrites it with the leader's entry.
