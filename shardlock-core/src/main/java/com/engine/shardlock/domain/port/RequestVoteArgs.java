package com.engine.shardlock.domain.port;

import com.engine.shardlock.domain.model.LogIndex;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.Term;

import java.util.Objects;

/**
 * Raft RequestVote arguments.
 */
public record RequestVoteArgs(
        Term term,
        NodeId candidateId,
        LogIndex lastLogIndex,
        Term lastLogTerm
) {
    public RequestVoteArgs {
        Objects.requireNonNull(term, "term cannot be null");
        Objects.requireNonNull(candidateId, "candidateId cannot be null");
        Objects.requireNonNull(lastLogIndex, "lastLogIndex cannot be null");
        Objects.requireNonNull(lastLogTerm, "lastLogTerm cannot be null");
    }
}
