package com.engine.shardlock.domain.model;

import java.util.Objects;

/**
 * An individual entry stored in the Raft log.
 */
public record LogEntry(
        LogIndex index,
        Term term,
        LockCommand command
) implements Comparable<LogEntry> {

    public LogEntry {
        Objects.requireNonNull(index, "LogIndex cannot be null");
        Objects.requireNonNull(term, "Term cannot be null");
        Objects.requireNonNull(command, "LockCommand cannot be null");
    }

    @Override
    public int compareTo(LogEntry o) {
        return this.index.compareTo(o.index);
    }
}
