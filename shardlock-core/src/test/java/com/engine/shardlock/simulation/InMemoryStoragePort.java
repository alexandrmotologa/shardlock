package com.engine.shardlock.simulation;

import com.engine.shardlock.domain.model.LogEntry;
import com.engine.shardlock.domain.model.LogIndex;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.Term;
import com.engine.shardlock.domain.port.StoragePort;
import com.engine.shardlock.domain.state.StateMachineSnapshot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory storage port implementation for deterministic unit testing.
 */
public class InMemoryStoragePort implements StoragePort {

    private StorageMetadata metadata = new StorageMetadata(Term.ZERO, null);
    private final NavigableMap<Long, LogEntry> entries = new TreeMap<>();
    private StateMachineSnapshot snapshot = null;

    @Override
    public synchronized void saveMetadata(Term currentTerm, NodeId votedFor) {
        this.metadata = new StorageMetadata(currentTerm, votedFor);
    }

    @Override
    public synchronized StorageMetadata readMetadata() {
        return metadata;
    }

    @Override
    public synchronized void appendEntries(List<LogEntry> newEntries) {
        for (LogEntry entry : newEntries) {
            entries.put(entry.index().value(), entry);
        }
    }

    @Override
    public synchronized void truncateSuffix(LogIndex fromIndex) {
        entries.tailMap(fromIndex.value(), true).clear();
    }

    @Override
    public synchronized List<LogEntry> readAllEntries() {
        return new ArrayList<>(entries.values());
    }

    @Override
    public synchronized void saveSnapshot(StateMachineSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public synchronized Optional<StateMachineSnapshot> readSnapshot() {
        return Optional.ofNullable(snapshot);
    }

    @Override
    public void close() {
    }
}
