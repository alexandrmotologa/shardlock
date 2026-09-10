package com.engine.shardlock.domain.port;

/**
 * Clock abstraction for deterministic time testing and distributed lease timers.
 */
public interface ClockPort {

    long currentTimeMillis();

    long nanoTime();

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
