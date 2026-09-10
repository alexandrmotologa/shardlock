package com.engine.shardlock.domain.model;

/**
 * Monotonically increasing index of an entry in the Raft log.
 * Raft uses 1-based indexing for committed entries. Index 0 represents the empty/initial state.
 */
public record LogIndex(long value) implements Comparable<LogIndex> {

    public static final LogIndex ZERO = new LogIndex(0);

    public LogIndex {
        if (value < 0) {
            throw new IllegalArgumentException("LogIndex cannot be negative: " + value);
        }
    }

    public static LogIndex of(long value) {
        return new LogIndex(value);
    }

    public LogIndex next() {
        return new LogIndex(value + 1);
    }

    public LogIndex prev() {
        if (value == 0) {
            return ZERO;
        }
        return new LogIndex(value - 1);
    }

    public boolean isGreaterThan(LogIndex other) {
        return this.value > other.value;
    }

    public boolean isLessThan(LogIndex other) {
        return this.value < other.value;
    }

    @Override
    public int compareTo(LogIndex o) {
        return Long.compare(this.value, o.value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
