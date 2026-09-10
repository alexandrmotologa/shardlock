package com.engine.shardlock.domain.port;

/**
 * Clock abstraction for deterministic time testing and distributed lease timers.
 */
@FunctionalInterface
public interface ClockPort {

    long currentTimeMillis();

    default long nanoTime() {
        return currentTimeMillis() * 1_000_000L;
    }

    static ClockPort system() {
        return new ClockPort() {
            @Override
            public long currentTimeMillis() {
                return System.currentTimeMillis();
            }

            @Override
            public long nanoTime() {
                return System.nanoTime();
            }
        };
    }
}
