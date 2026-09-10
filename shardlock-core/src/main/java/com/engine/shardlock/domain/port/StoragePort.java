package com.engine.shardlock.domain.port;

import com.engine.shardlock.domain.model.LogEntry;
import com.engine.shardlock.domain.model.LogIndex;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.model.Term;
import com.engine.shardlock.domain.state.StateMachineSnapshot;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * Port for persistent storage of Raft metadata, write-ahead logs, and snapshots.
 */
public interface StoragePort extends Closeable {

    record StorageMetadata(Term currentTerm, NodeId votedFor) {}

    void saveMetadata(Term currentTerm, NodeId votedFor);

    StorageMetadata readMetadata();

    void appendEntries(List<LogEntry> entries);

    void truncateSuffix(LogIndex fromIndex);

    List<LogEntry> readAllEntries();

    void saveSnapshot(StateMachineSnapshot snapshot);

    Optional<StateMachineSnapshot> readSnapshot();

    @Override
    void close();
}
