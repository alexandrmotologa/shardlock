package com.engine.shardlock.domain.model;

import java.util.Objects;

/**
 * Replicated state machine command carrying lock actions, lease metadata, and access modes.
 */
public record LockCommand(
        CommandType type,
        String resource,
        String clientId,
        long fencingToken,
        long ttlMs,
        long timestampMs,
        LockMode lockMode,
        int permits
) {
    public LockCommand {
        Objects.requireNonNull(type, "CommandType cannot be null");
        resource = (resource == null) ? "" : resource;
        clientId = (clientId == null) ? "" : clientId;
        lockMode = (lockMode == null) ? LockMode.EXCLUSIVE : lockMode;
        if (permits <= 0) {
            permits = 1;
        }
    }

    public LockCommand(
            CommandType type,
            String resource,
            String clientId,
            long fencingToken,
            long ttlMs,
            long timestampMs
    ) {
        this(type, resource, clientId, fencingToken, ttlMs, timestampMs, LockMode.EXCLUSIVE, 1);
    }

    public static LockCommand noop(long timestampMs) {
        return new LockCommand(CommandType.NOOP, "", "", 0, 0, timestampMs, LockMode.EXCLUSIVE, 1);
    }

    public static LockCommand acquire(String resource, String clientId, long ttlMs, long timestampMs) {
        return new LockCommand(CommandType.ACQUIRE_LOCK, resource, clientId, 0, ttlMs, timestampMs, LockMode.EXCLUSIVE, 1);
    }

    public static LockCommand acquireShared(String resource, String clientId, long ttlMs, long timestampMs) {
        return new LockCommand(CommandType.ACQUIRE_SHARED_LOCK, resource, clientId, 0, ttlMs, timestampMs, LockMode.SHARED, 1);
    }

    public static LockCommand acquireSemaphore(String resource, String clientId, int permits, long ttlMs, long timestampMs) {
        return new LockCommand(CommandType.ACQUIRE_SEMAPHORE, resource, clientId, 0, ttlMs, timestampMs, LockMode.SHARED, permits);
    }

    public static LockCommand renew(String resource, String clientId, long fencingToken, long ttlMs, long timestampMs) {
        return new LockCommand(CommandType.RENEW_LOCK, resource, clientId, fencingToken, ttlMs, timestampMs, LockMode.EXCLUSIVE, 1);
    }

    public static LockCommand release(String resource, String clientId, long fencingToken, long timestampMs) {
        return new LockCommand(CommandType.RELEASE_LOCK, resource, clientId, fencingToken, 0, timestampMs, LockMode.EXCLUSIVE, 1);
    }

    public static LockCommand releaseSemaphore(String resource, String clientId, int permits, long timestampMs) {
        return new LockCommand(CommandType.RELEASE_SEMAPHORE, resource, clientId, 0, 0, timestampMs, LockMode.SHARED, permits);
    }

    public static LockCommand expire(String resource, String clientId, long fencingToken, long timestampMs) {
        return new LockCommand(CommandType.EXPIRE_LOCK, resource, clientId, fencingToken, 0, timestampMs, LockMode.EXCLUSIVE, 1);
    }
}
