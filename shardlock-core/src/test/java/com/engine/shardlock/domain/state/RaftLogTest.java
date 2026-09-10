package com.engine.shardlock.domain.state;

import com.engine.shardlock.domain.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RaftLogTest {

    private RaftLog log;

    @BeforeEach
    void setUp() {
        log = new RaftLog();
    }

    @Test
    @DisplayName("Appends entries with monotonic 1-based indexing")
    void testAppendEntries() {
        assertThat(log.lastIndex()).isEqualTo(LogIndex.ZERO);
        assertThat(log.lastTerm()).isEqualTo(Term.ZERO);

        LogEntry e1 = log.append(Term.of(1), LockCommand.noop(100));
        assertThat(e1.index()).isEqualTo(LogIndex.of(1));
        assertThat(log.lastIndex()).isEqualTo(LogIndex.of(1));
        assertThat(log.lastTerm()).isEqualTo(Term.of(1));

        LogEntry e2 = log.append(Term.of(1), LockCommand.acquire("p0", "c1", 1000, 150));
        assertThat(e2.index()).isEqualTo(LogIndex.of(2));
        assertThat(log.lastIndex()).isEqualTo(LogIndex.of(2));

        assertThat(log.getEntry(LogIndex.of(1))).contains(e1);
        assertThat(log.getEntry(LogIndex.of(2))).contains(e2);
        assertThat(log.getTerm(LogIndex.of(2))).contains(Term.of(1));
    }

    @Test
    @DisplayName("Truncates log suffix upon conflict")
    void testTruncateSuffix() {
        log.append(Term.of(1), LockCommand.noop(100)); // index 1
        log.append(Term.of(1), LockCommand.noop(101)); // index 2
        log.append(Term.of(1), LockCommand.noop(102)); // index 3

        assertThat(log.lastIndex()).isEqualTo(LogIndex.of(3));

        // Truncate from index 2
        log.truncateSuffix(LogIndex.of(2));

        assertThat(log.lastIndex()).isEqualTo(LogIndex.of(1));
        assertThat(log.getEntry(LogIndex.of(2))).isEmpty();
        assertThat(log.getEntry(LogIndex.of(3))).isEmpty();
    }

    @Test
    @DisplayName("Compacts log up to snapshot index and term")
    void testLogCompaction() {
        log.append(Term.of(1), LockCommand.noop(100)); // 1
        log.append(Term.of(1), LockCommand.noop(101)); // 2
        log.append(Term.of(2), LockCommand.noop(102)); // 3
        log.append(Term.of(2), LockCommand.noop(103)); // 4

        log.compact(LogIndex.of(2), Term.of(1));

        assertThat(log.snapshotIndex()).isEqualTo(LogIndex.of(2));
        assertThat(log.snapshotTerm()).isEqualTo(Term.of(1));
        assertThat(log.getEntry(LogIndex.of(1))).isEmpty();
        assertThat(log.getEntry(LogIndex.of(2))).isEmpty();
        assertThat(log.getEntry(LogIndex.of(3))).isPresent();
        assertThat(log.getEntry(LogIndex.of(4))).isPresent();
        assertThat(log.lastIndex()).isEqualTo(LogIndex.of(4));
    }
}
