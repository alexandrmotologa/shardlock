package com.engine.shardlock.domain.model;

/**
 * Access mode for a distributed resource lock.
 */
public enum LockMode {
    /**
     * Exclusive mutual exclusion (standard write lock). Only one client can hold the lease.
     */
    EXCLUSIVE,

    /**
     * Shared access (read lock). Multiple clients can hold concurrent leases simultaneously.
     */
    SHARED
}
