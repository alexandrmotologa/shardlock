package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Shared Read vs. Exclusive Write Lock Verification")
class SharedLockTest {

    private RaftStateMachine sm;

    @BeforeEach
    void setUp() {
        sm = new RaftStateMachine();
    }

    @Test
    @DisplayName("Multiple clients can concurrently hold SHARED locks on the same partition")
    void concurrentSharedReadersPermitted() {
        LogEntry entry1 = new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquireShared("partition-0", "reader-1", 10000, 1000));
        LogEntry entry2 = new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.acquireShared("partition-0", "reader-2", 10000, 1000));

        StateMachineResult res1 = sm.apply(entry1);
        StateMachineResult res2 = sm.apply(entry2);

        assertTrue(res1.isSuccess());
        assertTrue(res2.isSuccess());
        assertEquals(LockMode.SHARED, res1.lockRecord().lockMode());
        assertEquals(LockMode.SHARED, res2.lockRecord().lockMode());

        assertEquals(2, sm.getAllLockRecords().size());
    }

    @Test
    @DisplayName("EXCLUSIVE write lock is rejected when SHARED read lock is active")
    void exclusiveLockBlockedByActiveSharedLock() {
        LogEntry readEntry = new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquireShared("partition-0", "reader-1", 10000, 1000));
        StateMachineResult readRes = sm.apply(readEntry);
        assertTrue(readRes.isSuccess());

        // Writer tries to acquire exclusive lock
        LogEntry writeEntry = new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.acquire("partition-0", "writer-1", 10000, 1000));
        StateMachineResult writeRes = sm.apply(writeEntry);

        assertFalse(writeRes.isSuccess());
        assertEquals(StateMachineResult.Status.REJECTED_ALREADY_HELD, writeRes.status());
    }

    @Test
    @DisplayName("Releasing all SHARED locks unblocks EXCLUSIVE write lock")
    void releasingSharedLocksAllowsExclusiveLock() {
        LogEntry readEntry = new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquireShared("partition-0", "reader-1", 10000, 1000));
        StateMachineResult readRes = sm.apply(readEntry);
        assertTrue(readRes.isSuccess());

        // Release reader-1
        LogEntry releaseEntry = new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.release("partition-0", "reader-1", readRes.lockRecord().fencingToken().value(), 1000));
        StateMachineResult relRes = sm.apply(releaseEntry);
        assertTrue(relRes.isSuccess());

        // Now writer-1 succeeds
        LogEntry writeEntry = new LogEntry(LogIndex.of(3), Term.of(1), LockCommand.acquire("partition-0", "writer-1", 10000, 1000));
        StateMachineResult writeRes = sm.apply(writeEntry);

        assertTrue(writeRes.isSuccess());
        assertEquals(LockMode.EXCLUSIVE, writeRes.lockRecord().lockMode());
    }
}
