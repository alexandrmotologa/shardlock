package com.engine.shardlock.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Downstream storage protection guard that prevents split-brain writes by rejecting
 * write operations bearing stale or lagging fencing tokens.
 */
public class FencingTokenGuard {

    private final Map<String, Long> highestSeenTokens = new ConcurrentHashMap<>();

    public synchronized <T> T executeWithToken(String resource, long fencingToken, Supplier<T> writeOperation) {
        long currentHighest = highestSeenTokens.getOrDefault(resource, 0L);

        if (fencingToken <= currentHighest) {
            throw new StaleFencingTokenException(resource, fencingToken, currentHighest);
        }

        // Token is valid and strictly higher: record new high-water mark and execute
        highestSeenTokens.put(resource, fencingToken);
        return writeOperation.get();
    }

    public synchronized void executeWithToken(String resource, long fencingToken, Runnable writeOperation) {
        executeWithToken(resource, fencingToken, () -> {
            writeOperation.run();
            return null;
        });
    }

    public long getHighestToken(String resource) {
        return highestSeenTokens.getOrDefault(resource, 0L);
    }

    public static class StaleFencingTokenException extends RuntimeException {
        public StaleFencingTokenException(String resource, long attemptedToken, long currentHighest) {
            super("Rejected stale write on resource '" + resource + "': attempted token " + attemptedToken
                    + " is not strictly greater than current high-water mark " + currentHighest);
        }
    }
}
