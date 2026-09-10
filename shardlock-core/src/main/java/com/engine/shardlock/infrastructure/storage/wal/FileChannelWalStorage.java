package com.engine.shardlock.infrastructure.storage.wal;

import com.engine.shardlock.domain.model.*;
import com.engine.shardlock.domain.port.StoragePort;
import com.engine.shardlock.domain.state.LockRecord;
import com.engine.shardlock.domain.state.StateMachineSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

/**
 * High-performance Write-Ahead Log (WAL) and snapshot persistence engine using
 * FileChannel with CRC32 integrity checks, torn-write truncation, and atomic metadata sync.
 */
public class FileChannelWalStorage implements StoragePort {

    private static final Logger log = LoggerFactory.getLogger(FileChannelWalStorage.class);

    private static final short RECORD_MAGIC_HEADER = 0x5741; // 'WA'
    private static final short RECORD_MAGIC_FOOTER = 0x4C31; // 'L1'

    private final Path dataDir;
    private final Path metaPath;
    private final Path walPath;
    private final Path snapshotPath;

    private final ObjectMapper mapper = new ObjectMapper();
    private FileChannel walChannel;
    private final List<LogEntry> cachedEntries = new ArrayList<>();
    private final List<Long> entryOffsets = new ArrayList<>(); // file offset of each entry

    public FileChannelWalStorage(Path dataDir) {
        this.dataDir = Objects.requireNonNull(dataDir);
        this.metaPath = dataDir.resolve("meta.properties");
        this.walPath = dataDir.resolve("wal.log");
        this.snapshotPath = dataDir.resolve("snapshot.json");

        try {
            Files.createDirectories(dataDir);
            initWal();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize WAL storage directory: " + dataDir, e);
        }
    }

    private synchronized void initWal() throws IOException {
        boolean walExists = Files.exists(walPath);
        this.walChannel = FileChannel.open(
                walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        if (walExists && walChannel.size() > 0) {
            recoverEntries();
        }
    }

    private void recoverEntries() throws IOException {
        walChannel.position(0);
        long fileSize = walChannel.size();
        long lastValidPosition = 0;

        CRC32 crc = new CRC32();
        ByteBuffer headerBuffer = ByteBuffer.allocate(2 + 4 + 8); // magic (2) + length (4) + checksum (8)

        while (walChannel.position() + 14 <= fileSize) {
            long entryStartPos = walChannel.position();
            headerBuffer.clear();
            int read = walChannel.read(headerBuffer);
            if (read < 14) {
                break;
            }
            headerBuffer.flip();
            short magic = headerBuffer.getShort();
            if (magic != RECORD_MAGIC_HEADER) {
                log.warn("Corrupted WAL record magic header at position {}. Truncating remainder.", entryStartPos);
                break;
            }
            int payloadLength = headerBuffer.getInt();
            long expectedCrc = headerBuffer.getLong();

            if (payloadLength < 0 || walChannel.position() + payloadLength + 2 > fileSize) {
                log.warn("Truncated or incomplete WAL payload at position {}. Truncating.", entryStartPos);
                break;
            }

            ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLength);
            walChannel.read(payloadBuffer);
            payloadBuffer.flip();

            crc.reset();
            crc.update(payloadBuffer.array(), 0, payloadLength);
            if (crc.getValue() != expectedCrc) {
                log.warn("CRC32 mismatch at position {}. Expected: {}, calculated: {}. Truncating.",
                        entryStartPos, expectedCrc, crc.getValue());
                break;
            }

            ByteBuffer footerBuffer = ByteBuffer.allocate(2);
            walChannel.read(footerBuffer);
            footerBuffer.flip();
            short footerMagic = footerBuffer.getShort();
            if (footerMagic != RECORD_MAGIC_FOOTER) {
                log.warn("Corrupted WAL record footer magic at position {}. Truncating.", entryStartPos);
                break;
            }

            String json = new String(payloadBuffer.array(), StandardCharsets.UTF_8);
            try {
                LogEntryDto dto = mapper.readValue(json, LogEntryDto.class);
                LogEntry entry = dto.toDomain();
                cachedEntries.add(entry);
                entryOffsets.add(entryStartPos);
                lastValidPosition = walChannel.position();
            } catch (Exception e) {
                log.warn("Failed to deserialize log entry JSON at position {}. Truncating: {}", entryStartPos, e.getMessage());
                break;
            }
        }

        // Truncate any uncommitted torn write at the end of the file
        if (lastValidPosition < fileSize) {
            log.info("Truncating torn write from offset {} to {}", lastValidPosition, fileSize);
            walChannel.truncate(lastValidPosition);
        }
        walChannel.position(lastValidPosition);
    }

    @Override
    public synchronized void saveMetadata(Term currentTerm, NodeId votedFor) {
        Properties props = new Properties();
        props.setProperty("currentTerm", String.valueOf(currentTerm.value()));
        props.setProperty("votedFor", votedFor != null ? votedFor.value() : "");

        Path tempPath = dataDir.resolve("meta.properties.tmp");
        try {
            try (FileOutputStream out = new FileOutputStream(tempPath.toFile())) {
                props.store(out, "ShardLock Node Metadata");
                out.getFD().sync();
            }
            Files.move(tempPath, metaPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist node metadata", e);
        }
    }

    @Override
    public synchronized StorageMetadata readMetadata() {
        if (!Files.exists(metaPath)) {
            return new StorageMetadata(Term.ZERO, null);
        }

        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(metaPath.toFile())) {
            props.load(in);
            long termVal = Long.parseLong(props.getProperty("currentTerm", "0"));
            String votedForVal = props.getProperty("votedFor", "");
            NodeId votedFor = votedForVal.isBlank() ? null : NodeId.of(votedForVal);
            return new StorageMetadata(Term.of(termVal), votedFor);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read node metadata", e);
        }
    }

    @Override
    public synchronized void appendEntries(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        CRC32 crc = new CRC32();
        try {
            for (LogEntry entry : entries) {
                long offset = walChannel.position();
                LogEntryDto dto = LogEntryDto.fromDomain(entry);
                byte[] jsonBytes = mapper.writeValueAsBytes(dto);

                crc.reset();
                crc.update(jsonBytes);
                long crcVal = crc.getValue();

                int totalRecordSize = 2 + 4 + 8 + jsonBytes.length + 2;
                ByteBuffer buf = ByteBuffer.allocate(totalRecordSize);
                buf.putShort(RECORD_MAGIC_HEADER);
                buf.putInt(jsonBytes.length);
                buf.putLong(crcVal);
                buf.put(jsonBytes);
                buf.putShort(RECORD_MAGIC_FOOTER);
                buf.flip();

                while (buf.hasRemaining()) {
                    walChannel.write(buf);
                }

                cachedEntries.add(entry);
                entryOffsets.add(offset);
            }
            walChannel.force(false); // fsync to durable storage
        } catch (IOException e) {
            throw new RuntimeException("Failed to write log entries to WAL", e);
        }
    }

    @Override
    public synchronized void truncateSuffix(LogIndex fromIndex) {
        int targetIdx = -1;
        for (int i = 0; i < cachedEntries.size(); i++) {
            if (!cachedEntries.get(i).index().isLessThan(fromIndex)) {
                targetIdx = i;
                break;
            }
        }

        if (targetIdx != -1) {
            long truncateOffset = entryOffsets.get(targetIdx);
            try {
                walChannel.truncate(truncateOffset);
                walChannel.position(truncateOffset);
                walChannel.force(false);

                // Trim in-memory caches
                while (cachedEntries.size() > targetIdx) {
                    cachedEntries.remove(cachedEntries.size() - 1);
                    entryOffsets.remove(entryOffsets.size() - 1);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to truncate WAL at index " + fromIndex, e);
            }
        }
    }

    @Override
    public synchronized List<LogEntry> readAllEntries() {
        return Collections.unmodifiableList(new ArrayList<>(cachedEntries));
    }

    @Override
    public synchronized void saveSnapshot(StateMachineSnapshot snapshot) {
        Path tempPath = dataDir.resolve("snapshot.json.tmp");
        try {
            SnapshotDto dto = SnapshotDto.fromDomain(snapshot);
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(dto);
            Files.write(tempPath, bytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

            Files.move(tempPath, snapshotPath, StandardCopyOption.REPLACE_EXISTING);

            // Once snapshot is written, we can compact the WAL up to lastIncludedIndex
            compactWal(snapshot.lastIncludedIndex());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save state machine snapshot", e);
        }
    }

    private void compactWal(LogIndex upToIndex) throws IOException {
        int keepFrom = -1;
        for (int i = 0; i < cachedEntries.size(); i++) {
            if (cachedEntries.get(i).index().isGreaterThan(upToIndex)) {
                keepFrom = i;
                break;
            }
        }

        if (keepFrom > 0) {
            List<LogEntry> toKeep = new ArrayList<>(cachedEntries.subList(keepFrom, cachedEntries.size()));
            walChannel.close();

            // Re-write remaining entries to fresh wal.log.tmp and swap
            Path tempWal = dataDir.resolve("wal.log.tmp");
            try (FileChannel tempChannel = FileChannel.open(
                    tempWal,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                CRC32 crc = new CRC32();
                for (LogEntry entry : toKeep) {
                    byte[] jsonBytes = mapper.writeValueAsBytes(LogEntryDto.fromDomain(entry));
                    crc.reset();
                    crc.update(jsonBytes);

                    ByteBuffer buf = ByteBuffer.allocate(16 + jsonBytes.length);
                    buf.putShort(RECORD_MAGIC_HEADER);
                    buf.putInt(jsonBytes.length);
                    buf.putLong(crc.getValue());
                    buf.put(jsonBytes);
                    buf.putShort(RECORD_MAGIC_FOOTER);
                    buf.flip();
                    while (buf.hasRemaining()) {
                        tempChannel.write(buf);
                    }
                }
                tempChannel.force(false);
            }

            Files.move(tempWal, walPath, StandardCopyOption.REPLACE_EXISTING);

            cachedEntries.clear();
            entryOffsets.clear();
            initWal();
        }
    }

    @Override
    public synchronized Optional<StateMachineSnapshot> readSnapshot() {
        if (!Files.exists(snapshotPath)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = Files.readAllBytes(snapshotPath);
            SnapshotDto dto = mapper.readValue(bytes, SnapshotDto.class);
            return Optional.of(dto.toDomain());
        } catch (IOException e) {
            log.warn("Failed to read snapshot file: {}", e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (walChannel != null && walChannel.isOpen()) {
                walChannel.force(false);
                walChannel.close();
            }
        } catch (IOException e) {
            log.warn("Error closing WAL channel: {}", e.getMessage());
        }
    }

    // DTOs for serialization
    public static class LogEntryDto {
        public long index;
        public long term;
        public String commandType;
        public String resource;
        public String clientId;
        public long fencingToken;
        public long ttlMs;
        public long timestampMs;

        public static LogEntryDto fromDomain(LogEntry entry) {
            LogEntryDto dto = new LogEntryDto();
            dto.index = entry.index().value();
            dto.term = entry.term().value();
            dto.commandType = entry.command().type().name();
            dto.resource = entry.command().resource();
            dto.clientId = entry.command().clientId();
            dto.fencingToken = entry.command().fencingToken();
            dto.ttlMs = entry.command().ttlMs();
            dto.timestampMs = entry.command().timestampMs();
            return dto;
        }

        public LogEntry toDomain() {
            CommandType ct = CommandType.valueOf(commandType);
            LockCommand cmd = new LockCommand(ct, resource, clientId, fencingToken, ttlMs, timestampMs);
            return new LogEntry(LogIndex.of(index), Term.of(term), cmd);
        }
    }

    public static class SnapshotDto {
        public long lastIncludedIndex;
        public long lastIncludedTerm;
        public long fencingCounter;
        public long createdAtMs;
        public Map<String, LockRecordDto> activeLocks = new HashMap<>();

        public static SnapshotDto fromDomain(StateMachineSnapshot snapshot) {
            SnapshotDto dto = new SnapshotDto();
            dto.lastIncludedIndex = snapshot.lastIncludedIndex().value();
            dto.lastIncludedTerm = snapshot.lastIncludedTerm().value();
            dto.fencingCounter = snapshot.fencingCounter();
            dto.createdAtMs = snapshot.createdAtMs();
            for (Map.Entry<String, LockRecord> e : snapshot.activeLocks().entrySet()) {
                LockRecord r = e.getValue();
                LockRecordDto rd = new LockRecordDto();
                rd.resource = r.resource();
                rd.ownerClientId = r.ownerClientId();
                rd.fencingToken = r.fencingToken().value();
                rd.acquiredAtMs = r.acquiredAtMs();
                rd.expiresAtMs = r.expiresAtMs();
                rd.ttlMs = r.ttlMs();
                dto.activeLocks.put(e.getKey(), rd);
            }
            return dto;
        }

        public StateMachineSnapshot toDomain() {
            Map<String, LockRecord> locks = new HashMap<>();
            for (Map.Entry<String, LockRecordDto> e : activeLocks.entrySet()) {
                LockRecordDto rd = e.getValue();
                LockRecord r = new LockRecord(
                        rd.resource,
                        rd.ownerClientId,
                        FencingToken.of(rd.fencingToken),
                        rd.acquiredAtMs,
                        rd.expiresAtMs,
                        rd.ttlMs
                );
                locks.put(e.getKey(), r);
            }
            return new StateMachineSnapshot(
                    LogIndex.of(lastIncludedIndex),
                    Term.of(lastIncludedTerm),
                    fencingCounter,
                    locks,
                    createdAtMs
            );
        }
    }

    public static class LockRecordDto {
        public String resource;
        public String ownerClientId;
        public long fencingToken;
        public long acquiredAtMs;
        public long expiresAtMs;
        public long ttlMs;
    }
}
