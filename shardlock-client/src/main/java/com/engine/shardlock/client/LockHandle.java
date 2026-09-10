package com.engine.shardlock.client;

import java.time.Duration;

/**
 * Handle representing an actively held partition lease and its monotonic fencing token.
 */
public record LockHandle(
        String resource,
        String clientId,
        long fencingToken,
        long expiresAtMs,
        Duration ttl
) {
    public boolean isExpired() {
        return System.currentTimeMillis() >= expiresAtMs;
    }

    public long remainingTtlMs() {
        return Math.max(0, expiresAtMs - System.currentTimeMillis());
    }
}
