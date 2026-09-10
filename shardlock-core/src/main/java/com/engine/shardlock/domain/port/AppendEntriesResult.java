package com.engine.shardlock.domain.port;

import com.engine.shardlock.domain.model.LogIndex;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.Term;

import java.util.Objects;

/**
 * Raft AppendEntries result.
 */
public record AppendEntriesResult(
        Term term,
        boolean success,
        LogIndex matchIndex,
        NodeId responderId
) {
    public AppendEntriesResult {
        Objects.requireNonNull(term, "term cannot be null");
        Objects.requireNonNull(matchIndex, "matchIndex cannot be null");
        Objects.requireNonNull(responderId, "responderId cannot be null");
    }
}
