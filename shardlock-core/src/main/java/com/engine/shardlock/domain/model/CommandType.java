package com.engine.shardlock.domain.model;

/**
 * Supported state machine command operations replicated through the Raft log.
 */
public enum CommandType {
    NOOP,
    ACQUIRE_LOCK,
    ACQUIRE_SHARED_LOCK,
    RENEW_LOCK,
    RELEASE_LOCK,
    EXPIRE_LOCK,
    ACQUIRE_SEMAPHORE,
    RELEASE_SEMAPHORE
}
