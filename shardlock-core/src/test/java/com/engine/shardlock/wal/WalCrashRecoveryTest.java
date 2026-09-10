package com.engine.shardlock.wal;

import com.engine.shardlock.domain.model.*;
import com.engine.shardlock.domain.port.StoragePort;
import com.engine.shardlock.domain.state.LockRecord;
import com.engine.shardlock.domain.state.StateMachineSnapshot;
import com.engine.shardlock.infrastructure.storage.wal.FileChannelWalStorage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class WalCrashRecoveryTest {

    @Test
    @DisplayName("WAL writes entries and recovers them cleanly across process restarts")
    void testWriteAndRecover(@TempDir Path tempDir) {
        try (FileChannelWalStorage storage = new FileChannelWalStorage(tempDir)) {
            storage.saveMetadata(Term.of(3), NodeId.of("node-2"));

            LogEntry e1 = new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquire("res-1", "worker-a", 10000, 100));
            LogEntry e2 = new LogEntry(LogIndex.of(2), Term.of(2), LockCommand.acquire("res-2", "worker-b", 5000, 200));

            storage.appendEntries(List.of(e1, e2));
        }

        // Restart process: open new instance on same data directory
        try (FileChannelWalStorage restarted = new FileChannelWalStorage(tempDir)) {
            StoragePort.StorageMetadata meta = restarted.readMetadata();
            assertThat(meta.currentTerm()).isEqualTo(Term.of(3));
            assertThat(meta.votedFor()).isEqualTo(NodeId.of("node-2"));

            List<LogEntry> entries = restarted.readAllEntries();
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).command().resource()).isEqualTo("res-1");
            assertThat(entries.get(1).command().resource()).isEqualTo("res-2");
        }
    }

    @Test
    @DisplayName("Truncates WAL suffix upon conflicting branch")
    void testTruncateSuffix(@TempDir Path tempDir) {
        try (FileChannelWalStorage storage = new FileChannelWalStorage(tempDir)) {
            LogEntry e1 = new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.noop(100));
            LogEntry e2 = new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.noop(200));
            LogEntry e3 = new LogEntry(LogIndex.of(3), Term.of(1), LockCommand.noop(300));
            storage.appendEntries(List.of(e1, e2, e3));

            storage.truncateSuffix(LogIndex.of(2));
            assertThat(storage.readAllEntries()).hasSize(1);
        }

        // Verify truncation persisted on disk
        try (FileChannelWalStorage restarted = new FileChannelWalStorage(tempDir)) {
            List<LogEntry> entries = restarted.readAllEntries();
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).index()).isEqualTo(LogIndex.of(1));
        }
    }

    @Test
    @DisplayName("Detects torn write at the end of WAL and truncates corrupted bytes to restore valid state")
    void testTornWriteRecovery(@TempDir Path tempDir) throws IOException {
        try (FileChannelWalStorage storage = new FileChannelWalStorage(tempDir)) {
            LogEntry e1 = new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquire("p0", "c1", 1000, 10));
            LogEntry e2 = new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.acquire("p1", "c2", 1000, 20));
            storage.appendEntries(List.of(e1, e2));
        }

        // Simulate a power failure / torn write by appending corrupt bytes to wal.log
        Path walFile = tempDir.resolve("wal.log");
        byte[] garbage = new byte[] { 0x57, 0x41, 0x00, 0x00, 0x12, 0x34, (byte) 0xDE, (byte) 0xAD };
        Files.write(walFile, garbage, StandardOpenOption.APPEND);

        // Recover: should detect corrupt record and cleanly recover e1 and e2
        try (FileChannelWalStorage restarted = new FileChannelWalStorage(tempDir)) {
            List<LogEntry> entries = restarted.readAllEntries();
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).index()).isEqualTo(LogIndex.of(1));
            assertThat(entries.get(1).index()).isEqualTo(LogIndex.of(2));
        }
    }

    @Test
    @DisplayName("Snapshots state machine and compacts WAL log")
    void testSnapshotAndCompaction(@TempDir Path tempDir) {
        try (FileChannelWalStorage storage = new FileChannelWalStorage(tempDir)) {
            LogEntry e1 = new LogEntry(LogIndex.of(1), Term.of(1), LockCommand.acquire("p0", "w1", 10000, 100));
            LogEntry e2 = new LogEntry(LogIndex.of(2), Term.of(1), LockCommand.acquire("p1", "w2", 10000, 200));
            LogEntry e3 = new LogEntry(LogIndex.of(3), Term.of(2), LockCommand.acquire("p2", "w3", 10000, 300));
            storage.appendEntries(List.of(e1, e2, e3));

            LockRecord r1 = new LockRecord("p0", "w1", FencingToken.of(1), 100, 10100, 10000);
            StateMachineSnapshot snapshot = new StateMachineSnapshot(
                    LogIndex.of(2),
                    Term.of(1),
                    2,
                    Map.of("p0", r1),
                    500
            );

            storage.saveSnapshot(snapshot);
        }

        // Restart and verify snapshot is loaded and WAL was compacted
        try (FileChannelWalStorage restarted = new FileChannelWalStorage(tempDir)) {
            Optional<StateMachineSnapshot> snapOpt = restarted.readSnapshot();
            assertThat(snapOpt).isPresent();
            StateMachineSnapshot snap = snapOpt.get();
            assertThat(snap.lastIncludedIndex()).isEqualTo(LogIndex.of(2));
            assertThat(snap.fencingCounter()).isEqualTo(2);
            assertThat(snap.activeLocks()).containsKey("p0");

            List<LogEntry> entries = restarted.readAllEntries();
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).index()).isEqualTo(LogIndex.of(3));
        }
    }
}
