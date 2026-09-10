package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RaftStateMachineTest {

    private RaftStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        stateMachine = new RaftStateMachine();
    }

    @Test
    @DisplayName("Acquiring a free resource issues fencing token 1 and records lock")
    void testAcquireFreeResource() {
        long now = 1000L;
        LockCommand cmd = LockCommand.acquire("partition-0", "worker-a", 5000L, now);
        LogEntry entry = new LogEntry(LogIndex.of(1), Term.of(1), cmd);

        StateMachineResult result = stateMachine.apply(entry);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.status()).isEqualTo(StateMachineResult.Status.SUCCESS);
        assertThat(result.fencingToken()).isEqualTo(FencingToken.of(1));

        Optional<LockRecord> recordOpt = stateMachine.getLock("partition-0");
        assertThat(recordOpt).isPresent();
        LockRecord record = recordOpt.get();
        assertThat(record.ownerClientId()).isEqualTo("worker-a");
        assertThat(record.fencingToken()).isEqualTo(FencingToken.of(1));
        assertThat(record.expiresAtMs()).isEqualTo(6000L);
    }

    @Test
    @DisplayName("Acquiring an already held unexpired resource by different worker is rejected")
    void testAcquireHeldResourceRejected() {
        long now = 1000L;
        stateMachine.apply(new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquire("partition-0", "worker-a", 5000L, now)));

        // Worker B tries to acquire partition-0 before it expires
        StateMachineResult result = stateMachine.apply(new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.acquire("partition-0", "worker-b", 5000L, now + 1000L)));

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.status()).isEqualTo(StateMachineResult.Status.REJECTED_ALREADY_HELD);
        assertThat(stateMachine.getLock("partition-0").get().ownerClientId()).isEqualTo("worker-a");
    }

    @Test
    @DisplayName("Acquiring after expiration succeeds and grants monotonic fencing token 2")
    void testAcquireAfterExpiration() {
        long now = 1000L;
        stateMachine.apply(new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquire("partition-0", "worker-a", 2000L, now)));

        // Time moves forward past expiration (3000ms > 1000 + 2000)
        long expiredTime = 4000L;
        StateMachineResult result = stateMachine.apply(new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.acquire("partition-0", "worker-b", 5000L, expiredTime)));

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.fencingToken()).isEqualTo(FencingToken.of(2));
        assertThat(stateMachine.getLock("partition-0").get().ownerClientId()).isEqualTo("worker-b");
    }

    @Test
    @DisplayName("Renewing an active lease extends expiration")
    void testRenewActiveLease() {
        long now = 1000L;
        StateMachineResult initial = stateMachine.apply(new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquire("partition-0", "worker-a", 5000L, now)));

        long renewTime = 3000L;
        LockCommand renewCmd = LockCommand.renew("partition-0", "worker-a", initial.fencingToken().value(), 5000L, renewTime);
        StateMachineResult renewResult = stateMachine.apply(new LogEntry(LogIndex.of(2), Term.of(1), renewCmd));

        assertThat(renewResult.isSuccess()).isTrue();
        LockRecord record = stateMachine.getLock("partition-0").get();
        assertThat(record.expiresAtMs()).isEqualTo(8000L);
        assertThat(record.fencingToken()).isEqualTo(FencingToken.of(1));
    }

    @Test
    @DisplayName("Releasing lock allows immediate acquisition with monotonic token increment")
    void testReleaseAndReacquire() {
        long now = 1000L;
        StateMachineResult r1 = stateMachine.apply(new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquire("partition-0", "worker-a", 5000L, now)));

        LockCommand releaseCmd = LockCommand.release("partition-0", "worker-a", r1.fencingToken().value(), now + 500L);
        StateMachineResult releaseResult = stateMachine.apply(new LogEntry(LogIndex.of(2), Term.of(1), releaseCmd));
        assertThat(releaseResult.isSuccess()).isTrue();
        assertThat(stateMachine.getLock("partition-0")).isEmpty();

        // Worker B acquires partition-0 immediately
        StateMachineResult r2 = stateMachine.apply(new LogEntry(LogIndex.of(3), Term.of(1), LockCommand.acquire("partition-0", "worker-b", 5000L, now + 600L)));
        assertThat(r2.isSuccess()).isTrue();
        assertThat(r2.fencingToken().value()).isGreaterThan(r1.fencingToken().value());
    }

    @Test
    @DisplayName("Snapshot captures state and restore reloads active locks and token counter")
    void testSnapshotAndRestore() {
        stateMachine.apply(new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquire("p0", "w1", 10000L, 100L)));
        stateMachine.apply(new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.acquire("p1", "w2", 10000L, 200L)));

        StateMachineSnapshot snapshot = stateMachine.snapshot(LogIndex.of(2), Term.of(1), 500L);
        assertThat(snapshot.activeLocks()).hasSize(2);
        assertThat(snapshot.fencingCounter()).isEqualTo(2);

        RaftStateMachine newStateMachine = new RaftStateMachine();
        newStateMachine.restore(snapshot);

        assertThat(newStateMachine.getFencingCounter()).isEqualTo(2);
        assertThat(newStateMachine.getLock("p0")).isPresent();
        assertThat(newStateMachine.getLock("p1")).isPresent();
    }
}
