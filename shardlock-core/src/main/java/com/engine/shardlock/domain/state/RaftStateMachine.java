package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * State machine managing distributed partition leases and monotonic fencing tokens.
 * Driven deterministically by committed Raft log entries.
 */
public class RaftStateMachine {

    private final Map<String, LockRecord> locks = new ConcurrentHashMap<>();
    private final AtomicLong fencingCounter = new AtomicLong(0);

    public RaftStateMachine() {
    }

    public synchronized StateMachineResult apply(LogEntry entry) {
        LockCommand cmd = entry.command();
        long now = cmd.timestampMs();

        return switch (cmd.type()) {
            case NOOP -> StateMachineResult.noop();
            case ACQUIRE_LOCK -> handleAcquire(cmd, now);
            case RENEW_LOCK -> handleRenew(cmd, now);
            case RELEASE_LOCK -> handleRelease(cmd);
            case EXPIRE_LOCK -> handleExpire(cmd);
        };
    }

    private StateMachineResult handleAcquire(LockCommand cmd, long now) {
        String resource = cmd.resource();
        String clientId = cmd.clientId();
        long ttlMs = cmd.ttlMs();

        LockRecord existing = locks.get(resource);
        if (existing != null && !existing.isExpired(now)) {
            if (existing.ownerClientId().equals(clientId)) {
                // Re-entrant acquisition or renewal by the current owner
                long newExpiry = now + ttlMs;
                LockRecord renewed = existing.renew(newExpiry, ttlMs);
                locks.put(resource, renewed);
                return StateMachineResult.success(renewed);
            }
            return StateMachineResult.rejectedAlreadyHeld(existing);
        }

        // Generate next strictly monotonic fencing token
        long nextTokenValue = fencingCounter.incrementAndGet();
        FencingToken token = FencingToken.of(nextTokenValue);
        long expiresAtMs = now + ttlMs;

        LockRecord record = new LockRecord(resource, clientId, token, now, expiresAtMs, ttlMs);
        locks.put(resource, record);
        return StateMachineResult.success(record);
    }

    private StateMachineResult handleRenew(LockCommand cmd, long now) {
        String resource = cmd.resource();
        String clientId = cmd.clientId();
        long tokenValue = cmd.fencingToken();
        long ttlMs = cmd.ttlMs();

        LockRecord existing = locks.get(resource);
        if (existing == null) {
            return StateMachineResult.rejectedNotFound(resource);
        }

        if (!existing.ownerClientId().equals(clientId)) {
            return StateMachineResult.rejectedNotOwner(existing.ownerClientId(), clientId);
        }

        if (existing.fencingToken().value() != tokenValue) {
            return StateMachineResult.rejectedTokenMismatch(existing.fencingToken().value(), tokenValue);
        }

        if (existing.isExpired(now)) {
            locks.remove(resource);
            return StateMachineResult.rejectedNotFound(resource + " (expired)");
        }

        long newExpiry = now + ttlMs;
        LockRecord renewed = existing.renew(newExpiry, ttlMs);
        locks.put(resource, renewed);
        return StateMachineResult.success(renewed);
    }

    private StateMachineResult handleRelease(LockCommand cmd) {
        String resource = cmd.resource();
        String clientId = cmd.clientId();
        long tokenValue = cmd.fencingToken();

        LockRecord existing = locks.get(resource);
        if (existing == null) {
            return StateMachineResult.rejectedNotFound(resource);
        }

        if (!existing.ownerClientId().equals(clientId)) {
            return StateMachineResult.rejectedNotOwner(existing.ownerClientId(), clientId);
        }

        if (existing.fencingToken().value() != tokenValue) {
            return StateMachineResult.rejectedTokenMismatch(existing.fencingToken().value(), tokenValue);
        }

        locks.remove(resource);
        return StateMachineResult.released(existing.fencingToken());
    }

    private StateMachineResult handleExpire(LockCommand cmd) {
        String resource = cmd.resource();
        long tokenValue = cmd.fencingToken();

        LockRecord existing = locks.get(resource);
        if (existing != null) {
            if (tokenValue == 0 || existing.fencingToken().value() == tokenValue) {
                locks.remove(resource);
                return StateMachineResult.expired(resource, existing.fencingToken());
            }
        }
        return StateMachineResult.noop();
    }

    public synchronized List<LockRecord> getExpiredLocks(long currentTimeMs) {
        List<LockRecord> expired = new ArrayList<>();
        for (LockRecord record : locks.values()) {
            if (record.isExpired(currentTimeMs)) {
                expired.add(record);
            }
        }
        return Collections.unmodifiableList(expired);
    }

    public Optional<LockRecord> getLock(String resource) {
        LockRecord record = locks.get(resource);
        return Optional.ofNullable(record);
    }

    public Map<String, LockRecord> getAllLocks() {
        return Collections.unmodifiableMap(new HashMap<>(locks));
    }

    public long getFencingCounter() {
        return fencingCounter.get();
    }

    public synchronized StateMachineSnapshot snapshot(LogIndex lastIncludedIndex, Term lastIncludedTerm, long createdAtMs) {
        return new StateMachineSnapshot(
                lastIncludedIndex,
                lastIncludedTerm,
                fencingCounter.get(),
                new HashMap<>(locks),
                createdAtMs
        );
    }

    public synchronized void restore(StateMachineSnapshot snapshot) {
        this.locks.clear();
        this.locks.putAll(snapshot.activeLocks());
        this.fencingCounter.set(snapshot.fencingCounter());
    }
}
