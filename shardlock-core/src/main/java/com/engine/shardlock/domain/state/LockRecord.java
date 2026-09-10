package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.FencingToken;

import java.util.Objects;

/**
 * An active partition lease recorded in the state machine.
 */
public record LockRecord(
        String resource,
        String ownerClientId,
        FencingToken fencingToken,
        long acquiredAtMs,
        long expiresAtMs,
        long ttlMs
) {
    public LockRecord {
        Objects.requireNonNull(resource, "Resource cannot be null");
        Objects.requireNonNull(ownerClientId, "OwnerClientId cannot be null");
        Objects.requireNonNull(fencingToken, "FencingToken cannot be null");
    }

    public boolean isExpired(long currentTimeMs) {
        return currentTimeMs >= expiresAtMs;
    }

    public long remainingTtlMs(long currentTimeMs) {
        return Math.max(0, expiresAtMs - currentTimeMs);
    }

    public LockRecord renew(long newExpiresAtMs, long newTtlMs) {
        return new LockRecord(
                this.resource,
                this.ownerClientId,
                this.fencingToken,
                this.acquiredAtMs,
                newExpiresAtMs,
                newTtlMs
        );
    }
}
