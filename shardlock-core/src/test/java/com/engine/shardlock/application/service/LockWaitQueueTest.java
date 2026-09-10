package com.engine.shardlock.application.service;

import com.engine.shardlock.domain.model.FencingToken;
import com.engine.shardlock.domain.model.LockMode;
import com.engine.shardlock.domain.state.LockRecord;
import com.engine.shardlock.domain.state.StateMachineResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Asynchronous Lock Wait Queue Verification")
class LockWaitQueueTest {

    private LockWaitQueue queue;

    @BeforeEach
    void setUp() {
        queue = new LockWaitQueue();
    }

    @AfterEach
    void tearDown() {
        queue.close();
    }

    @Test
    @DisplayName("Immediate grant when resource is free")
    void immediateGrantWhenFree() throws Exception {
        LockRecord record = new LockRecord("part-1", "client-1", FencingToken.of(1), 1000, 11000, 10000, LockMode.EXCLUSIVE);
        StateMachineResult successResult = StateMachineResult.success(record);

        CompletableFuture<StateMachineResult> future = queue.acquireOrEnqueue(
                "part-1",
                "client-1",
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                LockMode.EXCLUSIVE,
                () -> CompletableFuture.completedFuture(successResult)
        );

        StateMachineResult res = future.get(1, TimeUnit.SECONDS);
        assertTrue(res.isSuccess());
        assertEquals("part-1", res.lockRecord().resource());
    }

    @Test
    @DisplayName("Enqueued waiter is woken up when resource is freed")
    void queuedWaiterWokenOnResourceFreed() throws Exception {
        LockRecord existing = new LockRecord("part-1", "client-1", FencingToken.of(1), 1000, 11000, 10000, LockMode.EXCLUSIVE);
        StateMachineResult rejected = StateMachineResult.rejectedAlreadyHeld(existing);

        // Client 2 attempts to acquire with 5s wait timeout
        CompletableFuture<StateMachineResult> waitFuture = queue.acquireOrEnqueue(
                "part-1",
                "client-2",
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                LockMode.EXCLUSIVE,
                () -> CompletableFuture.completedFuture(rejected)
        );

        assertFalse(waitFuture.isDone(), "Wait request must be enqueued");
        assertEquals(1, queue.getQueueLength("part-1"));

        // Free the resource and wake up client 2
        LockRecord client2Record = new LockRecord("part-1", "client-2", FencingToken.of(2), 2000, 12000, 10000, LockMode.EXCLUSIVE);
        queue.onResourceFreed("part-1", req -> CompletableFuture.completedFuture(StateMachineResult.success(client2Record)));

        StateMachineResult res = waitFuture.get(1, TimeUnit.SECONDS);
        assertTrue(res.isSuccess());
        assertEquals("client-2", res.lockRecord().ownerClientId());
        assertEquals(0, queue.getQueueLength("part-1"));
    }

    @Test
    @DisplayName("Wait request times out and returns original rejection if not freed")
    void waitRequestTimesOut() throws Exception {
        LockRecord existing = new LockRecord("part-1", "client-1", FencingToken.of(1), 1000, 11000, 10000, LockMode.EXCLUSIVE);
        StateMachineResult rejected = StateMachineResult.rejectedAlreadyHeld(existing);

        CompletableFuture<StateMachineResult> waitFuture = queue.acquireOrEnqueue(
                "part-1",
                "client-2",
                Duration.ofSeconds(10),
                Duration.ofMillis(100), // Short timeout
                LockMode.EXCLUSIVE,
                () -> CompletableFuture.completedFuture(rejected)
        );

        StateMachineResult res = waitFuture.get(1, TimeUnit.SECONDS);
        assertFalse(res.isSuccess());
        assertEquals(0, queue.getQueueLength("part-1"));
    }
}
