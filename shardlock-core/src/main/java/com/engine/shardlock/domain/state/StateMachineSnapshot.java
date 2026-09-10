package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.LogIndex;
import com.engine.shardlock.domain.model.Term;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Compact snapshot representation of the Raft state machine for log compaction and fast catch-up.
 */
public record StateMachineSnapshot(
        LogIndex lastIncludedIndex,
        Term lastIncludedTerm,
        long fencingCounter,
        Map<String, LockRecord> activeLocks,
        long createdAtMs
) {
    public StateMachineSnapshot {
        Objects.requireNonNull(lastIncludedIndex, "lastIncludedIndex cannot be null");
        Objects.requireNonNull(lastIncludedTerm, "lastIncludedTerm cannot be null");
        activeLocks = (activeLocks == null) ? Map.of() : Collections.unmodifiableMap(activeLocks);
    }
}
