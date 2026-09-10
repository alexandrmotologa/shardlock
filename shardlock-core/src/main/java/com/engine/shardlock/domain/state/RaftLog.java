package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.LockCommand;
import com.engine.shardlock.domain.model.LogEntry;
import com.engine.shardlock.domain.model.LogIndex;
import com.engine.shardlock.domain.model.Term;

import java.util.*;

/**
 * Sequential append-only Raft log supporting compaction, truncation on conflict,
 * and monotonic commit tracking.
 */
public class RaftLog {

    private final NavigableMap<Long, LogEntry> entries = new TreeMap<>();
    private LogIndex snapshotIndex = LogIndex.ZERO;
    private Term snapshotTerm = Term.ZERO;

    private LogIndex commitIndex = LogIndex.ZERO;
    private LogIndex lastApplied = LogIndex.ZERO;

    public RaftLog() {
    }

    public synchronized LogEntry append(Term term, LockCommand command) {
        LogIndex nextIndex = lastIndex().next();
        LogEntry entry = new LogEntry(nextIndex, term, command);
        entries.put(nextIndex.value(), entry);
        return entry;
    }

    public synchronized void appendRaw(LogEntry entry) {
        entries.put(entry.index().value(), entry);
    }

    public synchronized LogIndex lastIndex() {
        if (entries.isEmpty()) {
            return snapshotIndex;
        }
        return LogIndex.of(entries.lastKey());
    }

    public synchronized Term lastTerm() {
        if (entries.isEmpty()) {
            return snapshotTerm;
        }
        return entries.lastEntry().getValue().term();
    }

    public synchronized Optional<LogEntry> getEntry(LogIndex index) {
        if (index.value() == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(entries.get(index.value()));
    }

    public synchronized Optional<Term> getTerm(LogIndex index) {
        if (index.value() == 0) {
            return Optional.of(Term.ZERO);
        }
        if (index.equals(snapshotIndex)) {
            return Optional.of(snapshotTerm);
        }
        LogEntry entry = entries.get(index.value());
        return entry != null ? Optional.of(entry.term()) : Optional.empty();
    }

    public synchronized List<LogEntry> getEntriesFrom(LogIndex startIndex) {
        return getEntriesFrom(startIndex, Integer.MAX_VALUE);
    }

    public synchronized List<LogEntry> getEntriesFrom(LogIndex startIndex, int maxEntries) {
        if (startIndex.value() <= 0) {
            startIndex = LogIndex.of(1);
        }
        List<LogEntry> result = new ArrayList<>();
        for (Map.Entry<Long, LogEntry> entry : entries.tailMap(startIndex.value(), true).entrySet()) {
            result.add(entry.getValue());
            if (result.size() >= maxEntries) {
                break;
            }
        }
        return Collections.unmodifiableList(result);
    }

    public synchronized void truncateSuffix(LogIndex fromIndex) {
        if (fromIndex.value() <= snapshotIndex.value()) {
            throw new IllegalArgumentException("Cannot truncate beyond snapshot index: " + snapshotIndex);
        }
        entries.tailMap(fromIndex.value(), true).clear();
    }

    public synchronized void compact(LogIndex newSnapshotIndex, Term newSnapshotTerm) {
        if (newSnapshotIndex.isLessThan(this.snapshotIndex)) {
            return;
        }
        this.snapshotIndex = newSnapshotIndex;
        this.snapshotTerm = newSnapshotTerm;

        // Discard entries up to snapshotIndex
        entries.headMap(newSnapshotIndex.value(), true).clear();

        if (commitIndex.isLessThan(newSnapshotIndex)) {
            commitIndex = newSnapshotIndex;
        }
        if (lastApplied.isLessThan(newSnapshotIndex)) {
            lastApplied = newSnapshotIndex;
        }
    }

    public synchronized LogIndex commitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(LogIndex newCommitIndex) {
        if (newCommitIndex.isGreaterThan(this.commitIndex)) {
            this.commitIndex = newCommitIndex;
        }
    }

    public synchronized LogIndex lastApplied() {
        return lastApplied;
    }

    public synchronized void setLastApplied(LogIndex newLastApplied) {
        this.lastApplied = newLastApplied;
    }

    public synchronized LogIndex snapshotIndex() {
        return snapshotIndex;
    }

    public synchronized Term snapshotTerm() {
        return snapshotTerm;
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized boolean isEmpty() {
        return entries.isEmpty() && snapshotIndex.value() == 0;
    }

    public synchronized List<LogEntry> allEntries() {
        return Collections.unmodifiableList(new ArrayList<>(entries.values()));
    }
}
