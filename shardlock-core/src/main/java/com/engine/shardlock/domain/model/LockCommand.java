package com.engine.shardlock.domain.model;

import java.util.Objects;

/**
 * Replicated state machine command carrying lock actions and lease metadata.
 */
public record LockCommand(
        CommandType type,
        String resource,
        String clientId,
        long fencingToken,
        long ttlMs,
        long timestampMs
) {
    public LockCommand {
        Objects.requireNonNull(type, "CommandType cannot be null");
        resource = (resource == null) ? "" : resource;
        clientId = (clientId == null) ? "" : clientId;
    }

    public static LockCommand noop(long timestampMs) {
        return new LockCommand(CommandType.NOOP, "", "", 0, 0, timestampMs);
    }

    public static LockCommand acquire(String resource, String clientId, long ttlMs, long timestampMs) {
        return new LockCommand(CommandType.ACQUIRE_LOCK, resource, clientId, 0, ttlMs, timestampMs);
    }

    public static LockCommand renew(String resource, String clientId, long fencingToken, long ttlMs, long timestampMs) {
        return new LockCommand(CommandType.RENEW_LOCK, resource, clientId, fencingToken, ttlMs, timestampMs);
    }

    public static LockCommand release(String resource, String clientId, long fencingToken, long timestampMs) {
        return new LockCommand(CommandType.RELEASE_LOCK, resource, clientId, fencingToken, 0, timestampMs);
    }

    public static LockCommand expire(String resource, String clientId, long fencingToken, long timestampMs) {
        return new LockCommand(CommandType.EXPIRE_LOCK, resource, clientId, fencingToken, 0, timestampMs);
    }
}
