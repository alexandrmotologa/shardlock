package com.engine.shardlock.domain.port;

import com.engine.shardlock.domain.model.LogEntry;
import com.engine.shardlock.domain.model.LogIndex;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.Term;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Raft AppendEntries arguments for replication and heartbeats.
 */
public record AppendEntriesArgs(
        Term term,
        NodeId leaderId,
        LogIndex prevLogIndex,
        Term prevLogTerm,
        List<LogEntry> entries,
        LogIndex leaderCommit
) {
    public AppendEntriesArgs {
        Objects.requireNonNull(term, "term cannot be null");
        Objects.requireNonNull(leaderId, "leaderId cannot be null");
        Objects.requireNonNull(prevLogIndex, "prevLogIndex cannot be null");
        Objects.requireNonNull(prevLogTerm, "prevLogTerm cannot be null");
        Objects.requireNonNull(leaderCommit, "leaderCommit cannot be null");
        entries = (entries == null) ? List.of() : Collections.unmodifiableList(entries);
    }

    public boolean isHeartbeat() {
        return entries.isEmpty();
    }
}
