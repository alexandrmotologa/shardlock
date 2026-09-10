package com.engine.shardlock.application.service;

import com.engine.shardlock.domain.model.FencingToken;
import com.engine.shardlock.domain.model.LockCommand;
import com.engine.shardlock.domain.model.LockMode;
import com.engine.shardlock.domain.model.NodeRole;
import com.engine.shardlock.domain.port.ClockPort;
import com.engine.shardlock.domain.state.LockRecord;
import com.engine.shardlock.domain.state.StateMachineResult;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Coordinates linearizable lock operations, shared leases, distributed semaphores,
 * and asynchronous wait queue wake-ups through the Raft consensus engine.
 */
public class LockManagerService {

    private final RaftConsensusEngine consensusEngine;
    private final ClockPort clock;
    private final LockWaitQueue waitQueue;

    public LockManagerService(RaftConsensusEngine consensusEngine, ClockPort clock) {
        this(consensusEngine, clock, new LockWaitQueue());
    }

    public LockManagerService(RaftConsensusEngine consensusEngine, ClockPort clock, LockWaitQueue waitQueue) {
        this.consensusEngine = Objects.requireNonNull(consensusEngine);
        this.clock = Objects.requireNonNull(clock);
        this.waitQueue = Objects.requireNonNull(waitQueue);
    }

    public CompletableFuture<StateMachineResult> acquireLock(String resource, String clientId, Duration ttl) {
        return acquireLock(resource, clientId, ttl, Duration.ZERO, LockMode.EXCLUSIVE);
    }

    public CompletableFuture<StateMachineResult> acquireLock(
            String resource,
            String clientId,
            Duration ttl,
            Duration waitTimeout,
            LockMode mode
    ) {
        return waitQueue.acquireOrEnqueue(
                resource,
                clientId,
                ttl,
                waitTimeout,
                mode,
                () -> {
                    LockCommand cmd = (mode == LockMode.SHARED)
                            ? LockCommand.acquireShared(resource, clientId, ttl.toMillis(), clock.currentTimeMillis())
                            : LockCommand.acquire(resource, clientId, ttl.toMillis(), clock.currentTimeMillis());
                    return consensusEngine.propose(cmd);
                }
        );
    }

    public CompletableFuture<StateMachineResult> acquireSharedLock(
            String resource,
            String clientId,
            Duration ttl,
            Duration waitTimeout
    ) {
        return acquireLock(resource, clientId, ttl, waitTimeout, LockMode.SHARED);
    }

    public CompletableFuture<StateMachineResult> acquireSemaphore(
            String resource,
            String clientId,
            int permits,
            Duration ttl
    ) {
        LockCommand cmd = LockCommand.acquireSemaphore(
                resource,
                clientId,
                permits,
                ttl.toMillis(),
                clock.currentTimeMillis()
        );
        return consensusEngine.propose(cmd);
    }

    public CompletableFuture<StateMachineResult> releaseSemaphore(
            String resource,
            String clientId,
            int permits
    ) {
        LockCommand cmd = LockCommand.releaseSemaphore(
                resource,
                clientId,
                permits,
                clock.currentTimeMillis()
        );
        return consensusEngine.propose(cmd).whenComplete((res, ex) -> {
            if (ex == null && res.isSuccess()) {
                notifyWaitQueue(resource);
            }
        });
    }

    public CompletableFuture<StateMachineResult> renewLock(String resource, String clientId, long fencingToken, Duration ttl) {
        LockCommand cmd = LockCommand.renew(
                resource,
                clientId,
                fencingToken,
                ttl.toMillis(),
                clock.currentTimeMillis()
        );
        return consensusEngine.propose(cmd);
    }

    public CompletableFuture<StateMachineResult> releaseLock(String resource, String clientId, long fencingToken) {
        LockCommand cmd = LockCommand.release(
                resource,
                clientId,
                fencingToken,
                clock.currentTimeMillis()
        );
        return consensusEngine.propose(cmd).whenComplete((res, ex) -> {
            if (ex == null && res.isSuccess()) {
                notifyWaitQueue(resource);
            }
        });
    }

    public void reapExpiredLocks() {
        if (consensusEngine.role() != NodeRole.LEADER) {
            return;
        }

        long now = clock.currentTimeMillis();
        List<LockRecord> expired = consensusEngine.stateMachine().getExpiredLocks(now);
        for (LockRecord record : expired) {
            LockCommand cmd = LockCommand.expire(
                    record.resource(),
                    record.ownerClientId(),
                    record.fencingToken().value(),
                    now
            );
            consensusEngine.propose(cmd).whenComplete((res, ex) -> {
                if (ex == null && res.isSuccess()) {
                    notifyWaitQueue(record.resource());
                }
            });
        }
    }

    private void notifyWaitQueue(String resource) {
        waitQueue.onResourceFreed(resource, req -> {
            LockCommand retryCmd = (req.mode() == LockMode.SHARED)
                    ? LockCommand.acquireShared(req.resource(), req.clientId(), req.ttl().toMillis(), clock.currentTimeMillis())
                    : LockCommand.acquire(req.resource(), req.clientId(), req.ttl().toMillis(), clock.currentTimeMillis());
            return consensusEngine.propose(retryCmd);
        });
    }

    public Optional<LockRecord> getLock(String resource) {
        return consensusEngine.stateMachine().getLock(resource);
    }

    public Map<String, LockRecord> getAllLocks() {
        return consensusEngine.stateMachine().getAllLocks();
    }

    public long getFencingCounter() {
        return consensusEngine.stateMachine().getFencingCounter();
    }

    public LockWaitQueue waitQueue() {
        return waitQueue;
    }
}
