package com.engine.shardlock.domain.model;

/**
 * Raft election epoch term counter. Monotonically increasing across elections.
 */
public record Term(long value) implements Comparable<Term> {

    public static final Term ZERO = new Term(0);

    public Term {
        if (value < 0) {
            throw new IllegalArgumentException("Term cannot be negative: " + value);
        }
    }

    public static Term of(long value) {
        return new Term(value);
    }

    public Term next() {
        return new Term(value + 1);
    }

    public boolean isGreaterThan(Term other) {
        return this.value > other.value;
    }

    public boolean isLessThan(Term other) {
        return this.value < other.value;
    }

    @Override
    public int compareTo(Term o) {
        return Long.compare(this.value, o.value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
