package com.engine.shardlock.client;

import java.time.Duration;

/**
 * Handle representing an actively held partition lease, its monotonic fencing token,
 * and its access mode (EXCLUSIVE vs SHARED).
 */
public record LockHandle(
        String resource,
        String clientId,
        long fencingToken,
        long expiresAtMs,
        Duration ttl,
        String mode
) {
    public LockHandle(String resource, String clientId, long fencingToken, long expiresAtMs, Duration ttl) {
        this(resource, clientId, fencingToken, expiresAtMs, ttl, "EXCLUSIVE");
    }

    public boolean isExpired() {
        return System.currentTimeMillis() >= expiresAtMs;
    }

    public long remainingTtlMs() {
        return Math.max(0, expiresAtMs - System.currentTimeMillis());
    }
}
