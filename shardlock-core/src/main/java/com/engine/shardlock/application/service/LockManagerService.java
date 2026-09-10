package com.engine.shardlock.application.service;

import com.engine.shardlock.domain.model.FencingToken;
import com.engine.shardlock.domain.model.LockCommand;
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
 * Coordinates linearizable lock operations and session renewals through the Raft consensus engine.
 */
public class LockManagerService {

    private final RaftConsensusEngine consensusEngine;
    private final ClockPort clock;

    public LockManagerService(RaftConsensusEngine consensusEngine, ClockPort clock) {
        this.consensusEngine = Objects.requireNonNull(consensusEngine);
        this.clock = Objects.requireNonNull(clock);
    }

    public CompletableFuture<StateMachineResult> acquireLock(String resource, String clientId, Duration ttl) {
        LockCommand cmd = LockCommand.acquire(
                resource,
                clientId,
                ttl.toMillis(),
                clock.currentTimeMillis()
        );
        return consensusEngine.propose(cmd);
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
        return consensusEngine.propose(cmd);
    }

    public void reapExpiredLocks() {
        if (consensusEngine.role() != com.engine.shardlock.domain.model.NodeRole.LEADER) {
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
            consensusEngine.propose(cmd);
        }
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
}
