package com.engine.shardlock.infrastructure.transport.nio;

import com.engine.shardlock.domain.model.*;
import com.engine.shardlock.domain.port.*;
import com.engine.shardlock.infrastructure.storage.wal.FileChannelWalStorage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Peer-to-peer asynchronous TCP transport using length-delimited binary frames,
 * correlation multiplexing, and Java 21 Virtual Threads.
 */
public class AsyncTcpTransport implements TransportPort {

    private static final Logger log = LoggerFactory.getLogger(AsyncTcpTransport.class);

    private static final short MAGIC = 0x534C; // 'SL'
    private static final short TYPE_VOTE_REQ = 0x0001;
    private static final short TYPE_VOTE_RESP = 0x0002;
    private static final short TYPE_APPEND_REQ = 0x0003;
    private static final short TYPE_APPEND_RESP = 0x0004;

    private final NodeId localNodeId;
    private final int port;
    private final Map<NodeId, InetSocketAddress> peerAddresses;
    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private ServerSocket serverSocket;
    private RaftRpcHandler rpcHandler;

    private final AtomicLong correlationCounter = new AtomicLong(0);
    private final Map<Long, CompletableFuture<?>> pendingRequests = new ConcurrentHashMap<>();
    private final Map<NodeId, PeerConnection> outboundConnections = new ConcurrentHashMap<>();

    public AsyncTcpTransport(NodeId localNodeId, int port, Map<NodeId, InetSocketAddress> peerAddresses) {
        this.localNodeId = Objects.requireNonNull(localNodeId);
        this.port = port;
        this.peerAddresses = Map.copyOf(peerAddresses);

        startServer();
    }

    private void startServer() {
        try {
            this.serverSocket = new ServerSocket(port);
            log.info("[{}] AsyncTcpTransport listening on port {}", localNodeId, port);

            // Accept incoming connections on virtual threads
            Thread.ofVirtual().name("tcp-acceptor-" + localNodeId).start(() -> {
                while (running.get() && !serverSocket.isClosed()) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        clientSocket.setTcpNoDelay(true);
                        Thread.ofVirtual().name("tcp-inbound-" + clientSocket.getRemoteSocketAddress()).start(() -> {
                            handleInboundSocket(clientSocket);
                        });
                    } catch (IOException e) {
                        if (running.get()) {
                            log.debug("[{}] Server socket accept error: {}", localNodeId, e.getMessage());
                        }
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to bind peer transport port " + port, e);
        }
    }

    private void handleInboundSocket(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            while (running.get() && !socket.isClosed()) {
                short magic = in.readShort();
                if (magic != MAGIC) {
                    log.warn("[{}] Invalid magic bytes from {}: 0x{}", localNodeId, socket.getRemoteSocketAddress(), Integer.toHexString(magic));
                    break;
                }

                short type = in.readShort();
                long correlationId = in.readLong();
                int payloadLength = in.readInt();

                byte[] payload = new byte[payloadLength];
                in.readFully(payload);
                String json = new String(payload, StandardCharsets.UTF_8);

                if (rpcHandler == null) {
                    continue;
                }

                if (type == TYPE_VOTE_REQ) {
                    RequestVoteArgsDto dto = mapper.readValue(json, RequestVoteArgsDto.class);
                    RequestVoteResult result = rpcHandler.handleRequestVote(dto.toDomain());
                    byte[] respPayload = mapper.writeValueAsBytes(RequestVoteResultDto.fromDomain(result));
                    sendFrame(out, TYPE_VOTE_RESP, correlationId, respPayload);
                } else if (type == TYPE_APPEND_REQ) {
                    AppendEntriesArgsDto dto = mapper.readValue(json, AppendEntriesArgsDto.class);
                    AppendEntriesResult result = rpcHandler.handleAppendEntries(dto.toDomain());
                    byte[] respPayload = mapper.writeValueAsBytes(AppendEntriesResultDto.fromDomain(result));
                    sendFrame(out, TYPE_APPEND_RESP, correlationId, respPayload);
                }
            }
        } catch (EOFException ignored) {
            // Peer closed socket
        } catch (Exception e) {
            if (running.get()) {
                log.debug("[{}] Inbound connection closed: {}", localNodeId, e.getMessage());
            }
        }
    }

    private synchronized void sendFrame(DataOutputStream out, short type, long correlationId, byte[] payload) throws IOException {
        out.writeShort(MAGIC);
        out.writeShort(type);
        out.writeLong(correlationId);
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    @Override
    public void registerRpcHandler(RaftRpcHandler handler) {
        this.rpcHandler = handler;
    }

    @Override
    public CompletableFuture<RequestVoteResult> sendRequestVote(NodeId destination, RequestVoteArgs args) {
        CompletableFuture<RequestVoteResult> future = new CompletableFuture<>();
        long correlationId = correlationCounter.incrementAndGet();
        pendingRequests.put(correlationId, future);

        try {
            byte[] payload = mapper.writeValueAsBytes(RequestVoteArgsDto.fromDomain(args));
            PeerConnection conn = getOrCreateConnection(destination);
            conn.send(TYPE_VOTE_REQ, correlationId, payload);
        } catch (Exception e) {
            pendingRequests.remove(correlationId);
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public CompletableFuture<AppendEntriesResult> sendAppendEntries(NodeId destination, AppendEntriesArgs args) {
        CompletableFuture<AppendEntriesResult> future = new CompletableFuture<>();
        long correlationId = correlationCounter.incrementAndGet();
        pendingRequests.put(correlationId, future);

        try {
            byte[] payload = mapper.writeValueAsBytes(AppendEntriesArgsDto.fromDomain(args));
            PeerConnection conn = getOrCreateConnection(destination);
            conn.send(TYPE_APPEND_REQ, correlationId, payload);
        } catch (Exception e) {
            pendingRequests.remove(correlationId);
            future.completeExceptionally(e);
        }

        return future;
    }

    private PeerConnection getOrCreateConnection(NodeId destination) throws IOException {
        PeerConnection conn = outboundConnections.get(destination);
        if (conn != null && conn.isOpen()) {
            return conn;
        }

        InetSocketAddress address = peerAddresses.get(destination);
        if (address == null) {
            throw new IllegalArgumentException("Unknown peer address for: " + destination);
        }

        synchronized (outboundConnections) {
            conn = outboundConnections.get(destination);
            if (conn != null && conn.isOpen()) {
                return conn;
            }
            conn = new PeerConnection(destination, address);
            outboundConnections.put(destination, conn);
            return conn;
        }
    }

    @Override
    public synchronized void close() {
        running.set(false);
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            log.debug("Error closing server socket: {}", e.getMessage());
        }

        for (PeerConnection conn : outboundConnections.values()) {
            conn.close();
        }
        outboundConnections.clear();
        pendingRequests.clear();
    }

    private class PeerConnection {
        private final NodeId peerId;
        private final Socket socket;
        private final DataOutputStream out;
        private final DataInputStream in;

        public PeerConnection(NodeId peerId, InetSocketAddress address) throws IOException {
            this.peerId = peerId;
            this.socket = new Socket(address.getHostString(), address.getPort());
            this.socket.setTcpNoDelay(true);
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            // Start response reader virtual thread
            Thread.ofVirtual().name("tcp-reader-" + peerId).start(this::readResponses);
        }

        public synchronized void send(short type, long correlationId, byte[] payload) throws IOException {
            sendFrame(out, type, correlationId, payload);
        }

        public boolean isOpen() {
            return socket != null && !socket.isClosed() && socket.isConnected();
        }

        private void readResponses() {
            try {
                while (running.get() && isOpen()) {
                    short magic = in.readShort();
                    if (magic != MAGIC) {
                        break;
                    }
                    short type = in.readShort();
                    long correlationId = in.readLong();
                    int length = in.readInt();
                    byte[] payload = new byte[length];
                    in.readFully(payload);
                    String json = new String(payload, StandardCharsets.UTF_8);

                    CompletableFuture<?> future = pendingRequests.remove(correlationId);
                    if (future != null) {
                        if (type == TYPE_VOTE_RESP) {
                            RequestVoteResultDto dto = mapper.readValue(json, RequestVoteResultDto.class);
                            ((CompletableFuture<RequestVoteResult>) future).complete(dto.toDomain());
                        } else if (type == TYPE_APPEND_RESP) {
                            AppendEntriesResultDto dto = mapper.readValue(json, AppendEntriesResultDto.class);
                            ((CompletableFuture<AppendEntriesResult>) future).complete(dto.toDomain());
                        }
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    log.debug("[{}] Connection to {} lost: {}", localNodeId, peerId, e.getMessage());
                }
            } finally {
                close();
            }
        }

        public synchronized void close() {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException ignored) {
            }
            outboundConnections.remove(peerId, this);
        }
    }

    // DTOs for network framing serialization
    public static class RequestVoteArgsDto {
        public long term;
        public String candidateId;
        public long lastLogIndex;
        public long lastLogTerm;

        public static RequestVoteArgsDto fromDomain(RequestVoteArgs args) {
            RequestVoteArgsDto d = new RequestVoteArgsDto();
            d.term = args.term().value();
            d.candidateId = args.candidateId().value();
            d.lastLogIndex = args.lastLogIndex().value();
            d.lastLogTerm = args.lastLogTerm().value();
            return d;
        }

        public RequestVoteArgs toDomain() {
            return new RequestVoteArgs(Term.of(term), NodeId.of(candidateId), LogIndex.of(lastLogIndex), Term.of(lastLogTerm));
        }
    }

    public static class RequestVoteResultDto {
        public long term;
        public boolean voteGranted;
        public String voterId;

        public static RequestVoteResultDto fromDomain(RequestVoteResult res) {
            RequestVoteResultDto d = new RequestVoteResultDto();
            d.term = res.term().value();
            d.voteGranted = res.voteGranted();
            d.voterId = res.voterId().value();
            return d;
        }

        public RequestVoteResult toDomain() {
            return new RequestVoteResult(Term.of(term), voteGranted, NodeId.of(voterId));
        }
    }

    public static class AppendEntriesArgsDto {
        public long term;
        public String leaderId;
        public long prevLogIndex;
        public long prevLogTerm;
        public List<FileChannelWalStorage.LogEntryDto> entries = new ArrayList<>();
        public long leaderCommit;

        public static AppendEntriesArgsDto fromDomain(AppendEntriesArgs args) {
            AppendEntriesArgsDto d = new AppendEntriesArgsDto();
            d.term = args.term().value();
            d.leaderId = args.leaderId().value();
            d.prevLogIndex = args.prevLogIndex().value();
            d.prevLogTerm = args.prevLogTerm().value();
            for (LogEntry e : args.entries()) {
                d.entries.add(FileChannelWalStorage.LogEntryDto.fromDomain(e));
            }
            d.leaderCommit = args.leaderCommit().value();
            return d;
        }

        public AppendEntriesArgs toDomain() {
            List<LogEntry> domainEntries = new ArrayList<>();
            for (FileChannelWalStorage.LogEntryDto e : entries) {
                domainEntries.add(e.toDomain());
            }
            return new AppendEntriesArgs(
                    Term.of(term),
                    NodeId.of(leaderId),
                    LogIndex.of(prevLogIndex),
                    Term.of(prevLogTerm),
                    domainEntries,
                    LogIndex.of(leaderCommit)
            );
        }
    }

    public static class AppendEntriesResultDto {
        public long term;
        public boolean success;
        public long matchIndex;
        public String responderId;

        public static AppendEntriesResultDto fromDomain(AppendEntriesResult res) {
            AppendEntriesResultDto d = new AppendEntriesResultDto();
            d.term = res.term().value();
            d.success = res.success();
            d.matchIndex = res.matchIndex().value();
            d.responderId = res.responderId().value();
            return d;
        }

        public AppendEntriesResult toDomain() {
            return new AppendEntriesResult(Term.of(term), success, LogIndex.of(matchIndex), NodeId.of(responderId));
        }
    }
}
