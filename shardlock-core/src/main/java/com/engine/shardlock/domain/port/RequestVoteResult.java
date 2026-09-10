package com.engine.shardlock.domain.port;

import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.Term;

import java.util.Objects;

/**
 * Raft RequestVote result.
 */
public record RequestVoteResult(
        Term term,
        boolean voteGranted,
        NodeId voterId
) {
    public RequestVoteResult {
        Objects.requireNonNull(term, "term cannot be null");
        Objects.requireNonNull(voterId, "voterId cannot be null");
    }
}
