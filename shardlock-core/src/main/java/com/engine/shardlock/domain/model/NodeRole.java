package com.engine.shardlock.domain.model;

/**
 * Standard consensus roles in the Raft protocol.
 */
public enum NodeRole {
    LEADER,
    FOLLOWER,
    CANDIDATE
}
