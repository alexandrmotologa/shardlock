package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * State machine managing distributed partition leases, shared read locks,
 * and monotonic fencing tokens.
 */
public class RaftStateMachine {

    private final Map<String, LockRecord> exclusiveLocks = new ConcurrentHashMap<>();
    private final Map<String, Map<String, LockRecord>> sharedLocks = new ConcurrentHashMap<>();
    private final AtomicLong fencingCounter = new AtomicLong(0);

    public RaftStateMachine() {
    }

    public synchronized StateMachineResult apply(LogEntry entry) {
        LockCommand cmd = entry.command();
        long now = cmd.timestampMs();

        return switch (cmd.type()) {
            case NOOP -> StateMachineResult.noop();
            case ACQUIRE_LOCK -> handleAcquireExclusive(cmd, now);
            case ACQUIRE_SHARED_LOCK -> handleAcquireShared(cmd, now);
            case RENEW_LOCK -> handleRenew(cmd, now);
            case RELEASE_LOCK -> handleRelease(cmd);
            case EXPIRE_LOCK -> handleExpire(cmd);
            case ACQUIRE_SEMAPHORE -> handleAcquireShared(cmd, now);
            case RELEASE_SEMAPHORE -> handleRelease(cmd);
        };
    }

    private StateMachineResult handleAcquireExclusive(LockCommand cmd, long now) {
        String resource = cmd.resource();
        String clientId = cmd.clientId();
        long ttlMs = cmd.ttlMs();

        // 1. Check for active shared read locks
        Map<String, LockRecord> currentShared = sharedLocks.get(resource);
        if (currentShared != null) {
            cleanExpiredShared(currentShared, now);
            if (!currentShared.isEmpty()) {
                LockRecord firstShared = currentShared.values().iterator().next();
                return StateMachineResult.rejectedAlreadyHeld(firstShared);
            }
        }

        // 2. Check for active exclusive lock
        LockRecord existing = exclusiveLocks.get(resource);
        if (existing != null && !existing.isExpired(now)) {
            if (existing.ownerClientId().equals(clientId)) {
                long newExpiry = now + ttlMs;
                LockRecord renewed = existing.renew(newExpiry, ttlMs);
                exclusiveLocks.put(resource, renewed);
                return StateMachineResult.success(renewed);
            }
            return StateMachineResult.rejectedAlreadyHeld(existing);
        }

        // 3. Grant exclusive lock
        long nextTokenValue = fencingCounter.incrementAndGet();
        FencingToken token = FencingToken.of(nextTokenValue);
        long expiresAtMs = now + ttlMs;

        LockRecord record = new LockRecord(resource, clientId, token, now, expiresAtMs, ttlMs, LockMode.EXCLUSIVE);
        exclusiveLocks.put(resource, record);
        return StateMachineResult.success(record);
    }

    private StateMachineResult handleAcquireShared(LockCommand cmd, long now) {
        String resource = cmd.resource();
        String clientId = cmd.clientId();
        long ttlMs = cmd.ttlMs();

        // 1. Check if resource is held exclusively
        LockRecord existingExcl = exclusiveLocks.get(resource);
        if (existingExcl != null && !existingExcl.isExpired(now)) {
            return StateMachineResult.rejectedAlreadyHeld(existingExcl);
        }

        // 2. Add or renew shared lock
        Map<String, LockRecord> currentShared = sharedLocks.computeIfAbsent(resource, k -> new ConcurrentHashMap<>());
        cleanExpiredShared(currentShared, now);

        LockRecord existingClientLock = currentShared.get(clientId);
        if (existingClientLock != null && !existingClientLock.isExpired(now)) {
            long newExpiry = now + ttlMs;
            LockRecord renewed = existingClientLock.renew(newExpiry, ttlMs);
            currentShared.put(clientId, renewed);
            return StateMachineResult.success(renewed);
        }

        long nextTokenValue = fencingCounter.incrementAndGet();
        FencingToken token = FencingToken.of(nextTokenValue);
        long expiresAtMs = now + ttlMs;

        LockRecord record = new LockRecord(resource, clientId, token, now, expiresAtMs, ttlMs, LockMode.SHARED);
        currentShared.put(clientId, record);
        return StateMachineResult.success(record);
    }

    private StateMachineResult handleRenew(LockCommand cmd, long now) {
        String resource = cmd.resource();
        String clientId = cmd.clientId();
        long tokenValue = cmd.fencingToken();
        long ttlMs = cmd.ttlMs();

        // Check exclusive locks
        LockRecord excl = exclusiveLocks.get(resource);
        if (excl != null) {
            if (!excl.ownerClientId().equals(clientId)) {
                return StateMachineResult.rejectedNotOwner(excl.ownerClientId(), clientId);
            }
            if (excl.fencingToken().value() != tokenValue) {
                return StateMachineResult.rejectedTokenMismatch(excl.fencingToken().value(), tokenValue);
            }
            if (excl.isExpired(now)) {
                exclusiveLocks.remove(resource);
                return StateMachineResult.rejectedNotFound(resource + " (expired)");
            }
            long newExpiry = now + ttlMs;
            LockRecord renewed = excl.renew(newExpiry, ttlMs);
            exclusiveLocks.put(resource, renewed);
            return StateMachineResult.success(renewed);
        }

        // Check shared locks
        Map<String, LockRecord> currentShared = sharedLocks.get(resource);
        if (currentShared != null) {
            LockRecord sh = currentShared.get(clientId);
            if (sh != null) {
                if (sh.fencingToken().value() != tokenValue) {
                    return StateMachineResult.rejectedTokenMismatch(sh.fencingToken().value(), tokenValue);
                }
                if (sh.isExpired(now)) {
                    currentShared.remove(clientId);
                    return StateMachineResult.rejectedNotFound(resource + " (expired)");
                }
                long newExpiry = now + ttlMs;
                LockRecord renewed = sh.renew(newExpiry, ttlMs);
                currentShared.put(clientId, renewed);
                return StateMachineResult.success(renewed);
            }
        }

        return StateMachineResult.rejectedNotFound(resource);
    }

    private StateMachineResult handleRelease(LockCommand cmd) {
        String resource = cmd.resource();
        String clientId = cmd.clientId();
        long tokenValue = cmd.fencingToken();

        // Check exclusive
        LockRecord excl = exclusiveLocks.get(resource);
        if (excl != null && excl.ownerClientId().equals(clientId)) {
            if (tokenValue != 0 && excl.fencingToken().value() != tokenValue) {
                return StateMachineResult.rejectedTokenMismatch(excl.fencingToken().value(), tokenValue);
            }
            exclusiveLocks.remove(resource);
            return StateMachineResult.released(excl.fencingToken());
        }

        // Check shared
        Map<String, LockRecord> currentShared = sharedLocks.get(resource);
        if (currentShared != null) {
            LockRecord sh = currentShared.get(clientId);
            if (sh != null) {
                if (tokenValue != 0 && sh.fencingToken().value() != tokenValue) {
                    return StateMachineResult.rejectedTokenMismatch(sh.fencingToken().value(), tokenValue);
                }
                currentShared.remove(clientId);
                if (currentShared.isEmpty()) {
                    sharedLocks.remove(resource);
                }
                return StateMachineResult.released(sh.fencingToken());
            }
        }

        return StateMachineResult.rejectedNotFound(resource);
    }

    private StateMachineResult handleExpire(LockCommand cmd) {
        String resource = cmd.resource();
        long tokenValue = cmd.fencingToken();

        LockRecord excl = exclusiveLocks.get(resource);
        if (excl != null && (tokenValue == 0 || excl.fencingToken().value() == tokenValue)) {
            exclusiveLocks.remove(resource);
            return StateMachineResult.expired(resource, excl.fencingToken());
        }

        Map<String, LockRecord> currentShared = sharedLocks.get(resource);
        if (currentShared != null) {
            Iterator<Map.Entry<String, LockRecord>> it = currentShared.entrySet().iterator();
            while (it.hasNext()) {
                LockRecord sh = it.next().getValue();
                if (tokenValue == 0 || sh.fencingToken().value() == tokenValue) {
                    it.remove();
                    if (currentShared.isEmpty()) {
                        sharedLocks.remove(resource);
                    }
                    return StateMachineResult.expired(resource, sh.fencingToken());
                }
            }
        }

        return StateMachineResult.noop();
    }

    private void cleanExpiredShared(Map<String, LockRecord> map, long now) {
        map.values().removeIf(r -> r.isExpired(now));
    }

    public synchronized List<LockRecord> getExpiredLocks(long currentTimeMs) {
        List<LockRecord> expired = new ArrayList<>();
        for (LockRecord r : exclusiveLocks.values()) {
            if (r.isExpired(currentTimeMs)) {
                expired.add(r);
            }
        }
        for (Map<String, LockRecord> map : sharedLocks.values()) {
            for (LockRecord r : map.values()) {
                if (r.isExpired(currentTimeMs)) {
                    expired.add(r);
                }
            }
        }
        return Collections.unmodifiableList(expired);
    }

    public Optional<LockRecord> getLock(String resource) {
        LockRecord excl = exclusiveLocks.get(resource);
        if (excl != null) {
            return Optional.of(excl);
        }
        Map<String, LockRecord> currentShared = sharedLocks.get(resource);
        if (currentShared != null && !currentShared.isEmpty()) {
            return Optional.of(currentShared.values().iterator().next());
        }
        return Optional.empty();
    }

    public List<LockRecord> getAllLockRecords() {
        List<LockRecord> list = new ArrayList<>(exclusiveLocks.values());
        for (Map<String, LockRecord> map : sharedLocks.values()) {
            list.addAll(map.values());
        }
        return Collections.unmodifiableList(list);
    }

    public Map<String, LockRecord> getAllLocks() {
        Map<String, LockRecord> map = new HashMap<>(exclusiveLocks);
        for (Map<String, LockRecord> shared : sharedLocks.values()) {
            for (LockRecord r : shared.values()) {
                map.put(r.resource() + "#" + r.ownerClientId(), r);
            }
        }
        return Collections.unmodifiableMap(map);
    }

    public long getFencingCounter() {
        return fencingCounter.get();
    }

    public synchronized StateMachineSnapshot snapshot(LogIndex lastIncludedIndex, Term lastIncludedTerm, long createdAtMs) {
        Map<String, LockRecord> all = new HashMap<>(exclusiveLocks);
        for (Map<String, LockRecord> shared : sharedLocks.values()) {
            for (LockRecord r : shared.values()) {
                all.put(r.resource() + ":" + r.ownerClientId(), r);
            }
        }
        return new StateMachineSnapshot(
                lastIncludedIndex,
                lastIncludedTerm,
                fencingCounter.get(),
                all,
                createdAtMs
        );
    }

    public synchronized void restore(StateMachineSnapshot snapshot) {
        this.exclusiveLocks.clear();
        this.sharedLocks.clear();
        for (Map.Entry<String, LockRecord> e : snapshot.activeLocks().entrySet()) {
            LockRecord r = e.getValue();
            if (r.lockMode() == LockMode.SHARED) {
                sharedLocks.computeIfAbsent(r.resource(), k -> new ConcurrentHashMap<>()).put(r.ownerClientId(), r);
            } else {
                exclusiveLocks.put(r.resource(), r);
            }
        }
        this.fencingCounter.set(snapshot.fencingCounter());
    }
}
