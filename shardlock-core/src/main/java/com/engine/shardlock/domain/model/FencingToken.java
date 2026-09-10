package com.engine.shardlock.domain.model;

/**
 * Strictly monotonic 64-bit fencing token issued when a lease or lock is granted.
 * Downstream stores check this token to reject stale writes from lagging clients.
 */
public record FencingToken(long value) implements Comparable<FencingToken> {

    public static final FencingToken ZERO = new FencingToken(0);

    public FencingToken {
        if (value < 0) {
            throw new IllegalArgumentException("FencingToken cannot be negative: " + value);
        }
    }

    public static FencingToken of(long value) {
        return new FencingToken(value);
    }

    public FencingToken next() {
        return new FencingToken(value + 1);
    }

    public boolean isGreaterThan(FencingToken other) {
        return this.value > other.value;
    }

    @Override
    public int compareTo(FencingToken o) {
        return Long.compare(this.value, o.value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
