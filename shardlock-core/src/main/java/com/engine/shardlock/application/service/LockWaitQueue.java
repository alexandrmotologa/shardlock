package com.engine.shardlock.application.service;

import com.engine.shardlock.domain.model.LockMode;
import com.engine.shardlock.domain.state.StateMachineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Asynchronous FIFO wait queue supporting HTTP long-polling and wake-on-release
 * for contested partition locks.
 */
public class LockWaitQueue {

    private static final Logger log = LoggerFactory.getLogger(LockWaitQueue.class);

    public record WaitRequest(
            String resource,
            String clientId,
            Duration ttl,
            LockMode mode,
            CompletableFuture<StateMachineResult> future,
            long deadlineMs
    ) {
        public boolean isExpired() {
            return System.currentTimeMillis() >= deadlineMs;
        }
    }

    private final Map<String, ConcurrentLinkedQueue<WaitRequest>> queues = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());

    public CompletableFuture<StateMachineResult> acquireOrEnqueue(
            String resource,
            String clientId,
            Duration ttl,
            Duration waitTimeout,
            LockMode mode,
            Supplier<CompletableFuture<StateMachineResult>> acquireSupplier
    ) {
        CompletableFuture<StateMachineResult> resultFuture = new CompletableFuture<>();

        acquireSupplier.get().whenComplete((initialResult, ex) -> {
            if (ex != null) {
                resultFuture.completeExceptionally(ex);
                return;
            }

            if (initialResult.isSuccess() || waitTimeout.isZero() || waitTimeout.isNegative()) {
                resultFuture.complete(initialResult);
                return;
            }

            // Enqueue request for long-polling
            long deadline = System.currentTimeMillis() + waitTimeout.toMillis();
            WaitRequest waitRequest = new WaitRequest(resource, clientId, ttl, mode, resultFuture, deadline);

            queues.computeIfAbsent(resource, k -> new ConcurrentLinkedQueue<>()).add(waitRequest);
            log.debug("Enqueued wait request for client '{}' on resource '{}' with timeout {} ms",
                    clientId, resource, waitTimeout.toMillis());

            // Schedule timeout cancellation
            timer.schedule(() -> {
                if (!resultFuture.isDone()) {
                    ConcurrentLinkedQueue<WaitRequest> queue = queues.get(resource);
                    if (queue != null) {
                        queue.remove(waitRequest);
                    }
                    resultFuture.complete(initialResult);
                }
            }, waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        });

        return resultFuture;
    }

    public void onResourceFreed(
            String resource,
            Function<WaitRequest, CompletableFuture<StateMachineResult>> retryFunction
    ) {
        ConcurrentLinkedQueue<WaitRequest> queue = queues.get(resource);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        while (!queue.isEmpty()) {
            WaitRequest next = queue.peek();
            if (next == null) {
                break;
            }

            if (next.isExpired() || next.future().isDone()) {
                queue.poll();
                continue;
            }

            // Attempt acquisition for the waiting client
            retryFunction.apply(next).whenComplete((res, ex) -> {
                if (ex != null) {
                    next.future().completeExceptionally(ex);
                    queue.remove(next);
                } else if (res.isSuccess()) {
                    queue.remove(next);
                    next.future().complete(res);
                }
            });

            // For exclusive locks, only wake one valid waiter at a time
            if (next.mode() == LockMode.EXCLUSIVE) {
                break;
            }
        }
    }

    public int getQueueLength(String resource) {
        ConcurrentLinkedQueue<WaitRequest> queue = queues.get(resource);
        return queue != null ? queue.size() : 0;
    }

    public Map<String, Integer> getAllQueueLengths() {
        Map<String, Integer> map = new HashMap<>();
        for (Map.Entry<String, ConcurrentLinkedQueue<WaitRequest>> e : queues.entrySet()) {
            if (!e.getValue().isEmpty()) {
                map.put(e.getKey(), e.getValue().size());
            }
        }
        return Collections.unmodifiableMap(map);
    }

    public void close() {
        timer.shutdownNow();
    }
}
