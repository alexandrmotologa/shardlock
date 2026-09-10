package com.engine.shardlock.infrastructure.server.http;

import com.engine.shardlock.application.service.LockManagerService;
import com.engine.shardlock.application.service.RaftConsensusEngine;
import com.engine.shardlock.domain.event.RaftDomainEvent;
import com.engine.shardlock.domain.model.FencingToken;
import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.domain.state.LockRecord;
import com.engine.shardlock.domain.state.StateMachineResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lightweight, zero-dependency embedded HTTP and Server-Sent Events (SSE) server
 * running on Java 21 Virtual Threads.
 */
public class EmbeddedHttpServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedHttpServer.class);

    private final HttpServer server;
    private final int port;
    private final RaftConsensusEngine consensusEngine;
    private final LockManagerService lockManager;
    private final Map<NodeId, String> peerHttpUrls;
    private final ObjectMapper mapper = new ObjectMapper();

    private final List<OutputStream> sseClients = new CopyOnWriteArrayList<>();
    private final java.util.Deque<byte[]> recentEvents = new java.util.concurrent.ConcurrentLinkedDeque<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public EmbeddedHttpServer(
            int port,
            RaftConsensusEngine consensusEngine,
            LockManagerService lockManager,
            Map<NodeId, String> peerHttpUrls
    ) {
        this.port = port;
        this.consensusEngine = Objects.requireNonNull(consensusEngine);
        this.lockManager = Objects.requireNonNull(lockManager);
        this.peerHttpUrls = (peerHttpUrls == null) ? Map.of() : Map.copyOf(peerHttpUrls);

        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
            this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

            registerRoutes();
        } catch (IOException e) {
            throw new RuntimeException("Failed to bind HTTP server on port " + port, e);
        }
    }

    public void start() {
        server.start();
        log.info("[{}] EmbeddedHttpServer started on port {}", consensusEngine.nodeId(), port);
    }

    private void registerRoutes() {
        server.createContext("/api/v1/cluster/status", this::handleClusterStatus);
        server.createContext("/api/v1/locks/acquire", this::handleAcquireLock);
        server.createContext("/api/v1/locks/renew", this::handleRenewLock);
        server.createContext("/api/v1/locks/release", this::handleReleaseLock);
        server.createContext("/api/v1/locks/waiters", this::handleWaitQueue);
        server.createContext("/api/v1/locks", this::handleListLocks);
        server.createContext("/api/v1/cluster/events", this::handleSseEvents);
        server.createContext("/metrics", this::handleMetrics);
        server.createContext("/logo.png", this::handleLogo);
        server.createContext("/", this::handleDashboard);
    }

    private void handleClusterStatus(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        Map<String, Object> status = new LinkedHashMap<>();
        status.put("nodeId", consensusEngine.nodeId().value());
        status.put("role", consensusEngine.role().name());
        status.put("currentTerm", consensusEngine.currentTerm().value());
        status.put("leaderId", consensusEngine.currentLeaderId() != null ? consensusEngine.currentLeaderId().value() : null);
        status.put("commitIndex", consensusEngine.raftLog().commitIndex().value());
        status.put("lastApplied", consensusEngine.raftLog().lastApplied().value());
        status.put("fencingCounter", lockManager.getFencingCounter());
        status.put("peers", consensusEngine.peers().stream().map(NodeId::value).toList());
        status.put("activeLockCount", lockManager.getAllLocks().size());
        status.put("waitQueueCount", lockManager.waitQueue().getAllQueueLengths().values().stream().mapToInt(Integer::intValue).sum());

        sendJsonResponse(exchange, 200, status);
    }

    private void handleWaitQueue(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("queueLengths", lockManager.waitQueue().getAllQueueLengths());
        sendJsonResponse(exchange, 200, resp);
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("# HELP shardlock_raft_term Current Raft election term\n");
        sb.append("# TYPE shardlock_raft_term gauge\n");
        sb.append("shardlock_raft_term ").append(consensusEngine.currentTerm().value()).append("\n\n");

        sb.append("# HELP shardlock_raft_is_leader 1 if node is cluster leader, 0 otherwise\n");
        sb.append("# TYPE shardlock_raft_is_leader gauge\n");
        sb.append("shardlock_raft_is_leader ").append(consensusEngine.role() == com.engine.shardlock.domain.model.NodeRole.LEADER ? 1 : 0).append("\n\n");

        sb.append("# HELP shardlock_raft_commit_index Highest log index known to be committed\n");
        sb.append("# TYPE shardlock_raft_commit_index gauge\n");
        sb.append("shardlock_raft_commit_index ").append(consensusEngine.raftLog().commitIndex().value()).append("\n\n");

        sb.append("# HELP shardlock_raft_last_applied Highest log index applied to state machine\n");
        sb.append("# TYPE shardlock_raft_last_applied gauge\n");
        sb.append("shardlock_raft_last_applied ").append(consensusEngine.raftLog().lastApplied().value()).append("\n\n");

        sb.append("# HELP shardlock_locks_active_total Current count of active leases\n");
        sb.append("# TYPE shardlock_locks_active_total gauge\n");
        sb.append("shardlock_locks_active_total ").append(lockManager.getAllLocks().size()).append("\n\n");

        sb.append("# HELP shardlock_fencing_token_current Latest monotonically incremented fencing token\n");
        sb.append("# TYPE shardlock_fencing_token_current counter\n");
        sb.append("shardlock_fencing_token_current ").append(lockManager.getFencingCounter()).append("\n\n");

        int waitQueueSize = lockManager.waitQueue().getAllQueueLengths().values().stream().mapToInt(Integer::intValue).sum();
        sb.append("# HELP shardlock_wait_queue_length Total clients waiting in FIFO wait queue\n");
        sb.append("# TYPE shardlock_wait_queue_length gauge\n");
        sb.append("shardlock_wait_queue_length ").append(waitQueueSize).append("\n");

        byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private void handleListLocks(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        long now = System.currentTimeMillis();
        List<Map<String, Object>> list = new ArrayList<>();
        for (LockRecord r : lockManager.getAllLocks().values()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("resource", r.resource());
            item.put("ownerClientId", r.ownerClientId());
            item.put("fencingToken", r.fencingToken().value());
            item.put("acquiredAtMs", r.acquiredAtMs());
            item.put("expiresAtMs", r.expiresAtMs());
            item.put("ttlMs", r.ttlMs());
            item.put("remainingTtlMs", r.remainingTtlMs(now));
            item.put("expired", r.isExpired(now));
            item.put("mode", r.lockMode().name());
            list.add(item);
        }

        sendJsonResponse(exchange, 200, list);
    }

    private void handleAcquireLock(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        if (checkAndRedirectIfFollower(exchange)) {
            return;
        }

        try {
            Map<String, Object> req = parseJsonRequest(exchange);
            String resource = (String) req.get("resource");
            String clientId = (String) req.get("clientId");
            long ttlMs = ((Number) req.getOrDefault("ttlMs", 10000)).longValue();
            long waitTimeoutMs = ((Number) req.getOrDefault("waitTimeoutMs", 0)).longValue();
            String modeStr = (String) req.getOrDefault("mode", "EXCLUSIVE");
            com.engine.shardlock.domain.model.LockMode mode = "SHARED".equalsIgnoreCase(modeStr)
                    ? com.engine.shardlock.domain.model.LockMode.SHARED
                    : com.engine.shardlock.domain.model.LockMode.EXCLUSIVE;

            if (resource == null || resource.isBlank() || clientId == null || clientId.isBlank()) {
                sendJsonResponse(exchange, 400, Map.of("error", "resource and clientId are required"));
                return;
            }

            long timeoutLimit = Math.max(5000, waitTimeoutMs + 5000);
            StateMachineResult res = lockManager.acquireLock(
                    resource,
                    clientId,
                    Duration.ofMillis(ttlMs),
                    Duration.ofMillis(waitTimeoutMs),
                    mode
            ).get(timeoutLimit, TimeUnit.MILLISECONDS);

            if (res.isSuccess()) {
                LockRecord rec = res.lockRecord();
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("status", "ACQUIRED");
                resp.put("resource", rec.resource());
                resp.put("clientId", rec.ownerClientId());
                resp.put("fencingToken", rec.fencingToken().value());
                resp.put("expiresAtMs", rec.expiresAtMs());
                resp.put("ttlMs", rec.ttlMs());
                resp.put("mode", rec.lockMode().name());
                sendJsonResponse(exchange, 200, resp);
            } else {
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("status", res.status().name());
                resp.put("message", res.message());
                resp.put("currentFencingToken", res.fencingToken().value());
                sendJsonResponse(exchange, 409, resp);
            }
        } catch (Exception e) {
            sendJsonResponse(exchange, 500, Map.of("error", e.getMessage() != null ? e.getMessage() : "Unknown error"));
        }
    }

    private void handleRenewLock(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        if (checkAndRedirectIfFollower(exchange)) {
            return;
        }

        try {
            Map<String, Object> req = parseJsonRequest(exchange);
            String resource = (String) req.get("resource");
            String clientId = (String) req.get("clientId");
            long fencingToken = ((Number) req.get("fencingToken")).longValue();
            long ttlMs = ((Number) req.getOrDefault("ttlMs", 10000)).longValue();

            StateMachineResult res = lockManager.renewLock(resource, clientId, fencingToken, Duration.ofMillis(ttlMs)).get(5, TimeUnit.SECONDS);

            if (res.isSuccess()) {
                LockRecord rec = res.lockRecord();
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("status", "RENEWED");
                resp.put("resource", rec.resource());
                resp.put("clientId", rec.ownerClientId());
                resp.put("fencingToken", rec.fencingToken().value());
                resp.put("expiresAtMs", rec.expiresAtMs());
                resp.put("ttlMs", rec.ttlMs());
                sendJsonResponse(exchange, 200, resp);
            } else {
                sendJsonResponse(exchange, 409, Map.of("status", res.status().name(), "message", res.message()));
            }
        } catch (Exception e) {
            sendJsonResponse(exchange, 500, Map.of("error", e.getMessage() != null ? e.getMessage() : "Internal error"));
        }
    }

    private void handleReleaseLock(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        if (checkAndRedirectIfFollower(exchange)) {
            return;
        }

        try {
            Map<String, Object> req = parseJsonRequest(exchange);
            String resource = (String) req.get("resource");
            String clientId = (String) req.get("clientId");
            long fencingToken = ((Number) req.get("fencingToken")).longValue();

            StateMachineResult res = lockManager.releaseLock(resource, clientId, fencingToken).get(5, TimeUnit.SECONDS);

            if (res.isSuccess()) {
                sendJsonResponse(exchange, 200, Map.of("status", "RELEASED", "resource", resource, "fencingToken", fencingToken));
            } else {
                sendJsonResponse(exchange, 409, Map.of("status", res.status().name(), "message", res.message()));
            }
        } catch (Exception e) {
            sendJsonResponse(exchange, 500, Map.of("error", e.getMessage() != null ? e.getMessage() : "Internal error"));
        }
    }

    private boolean checkAndRedirectIfFollower(HttpExchange exchange) throws IOException {
        if (consensusEngine.role() != com.engine.shardlock.domain.model.NodeRole.LEADER) {
            NodeId leader = consensusEngine.currentLeaderId();
            Headers responseHeaders = exchange.getResponseHeaders();
            if (leader != null) {
                responseHeaders.set("X-Leader-Id", leader.value());
                String leaderHttp = peerHttpUrls.get(leader);
                if (leaderHttp != null) {
                    String redirectUrl = leaderHttp + exchange.getRequestURI().getPath();
                    responseHeaders.set("Location", redirectUrl);
                    exchange.sendResponseHeaders(307, -1);
                    return true;
                }
            }
            sendJsonResponse(exchange, 503, Map.of(
                    "error", "Node is not leader",
                    "leaderId", leader != null ? leader.value() : "unknown"
            ));
            return true;
        }
        return false;
    }

    private void handleSseEvents(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "text/event-stream");
        headers.set("Cache-Control", "no-cache");
        headers.set("Connection", "keep-alive");

        exchange.sendResponseHeaders(200, 0);
        OutputStream os = exchange.getResponseBody();
        sseClients.add(os);

        // Send initial connect ping
        String initData = "data: {\"event\": \"CONNECTED\", \"nodeId\": \"" + consensusEngine.nodeId().value() + "\"}\n\n";
        os.write(initData.getBytes(StandardCharsets.UTF_8));
        for (byte[] ev : recentEvents) {
            try {
                os.write(ev);
            } catch (IOException ignored) {}
        }
        os.flush();
    }

    public synchronized void publishEvent(RaftDomainEvent event) {
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("eventType", event.getClass().getSimpleName());
            payload.put("timestamp", System.currentTimeMillis());

            switch (event) {
                case RaftDomainEvent.RoleChangedEvent r -> {
                    payload.put("nodeId", r.nodeId().value());
                    payload.put("oldRole", r.oldRole().name());
                    payload.put("newRole", r.newRole().name());
                    payload.put("term", r.term().value());
                }
                case RaftDomainEvent.LeaderElectedEvent l -> {
                    payload.put("leaderId", l.leaderId().value());
                    payload.put("term", l.term().value());
                }
                case RaftDomainEvent.LockAcquiredEvent a -> {
                    payload.put("resource", a.lockRecord().resource());
                    payload.put("owner", a.lockRecord().ownerClientId());
                    payload.put("fencingToken", a.lockRecord().fencingToken().value());
                    payload.put("expiresAtMs", a.lockRecord().expiresAtMs());
                }
                case RaftDomainEvent.LockReleasedEvent rel -> {
                    payload.put("resource", rel.resource());
                    payload.put("fencingToken", rel.fencingToken().value());
                }
                case RaftDomainEvent.LockExpiredEvent exp -> {
                    payload.put("resource", exp.resource());
                    payload.put("fencingToken", exp.fencingToken().value());
                }
                default -> payload.put("info", event.toString());
            }

            String message = "data: " + mapper.writeValueAsString(payload) + "\n\n";
            byte[] bytes = message.getBytes(StandardCharsets.UTF_8);

            recentEvents.add(bytes);
            while (recentEvents.size() > 20) {
                recentEvents.pollFirst();
            }

            if (sseClients.isEmpty()) {
                return;
            }

            List<OutputStream> dead = new ArrayList<>();
            for (OutputStream os : sseClients) {
                try {
                    os.write(bytes);
                    os.flush();
                } catch (IOException e) {
                    dead.add(os);
                }
            }
            sseClients.removeAll(dead);
        } catch (Exception e) {
            log.debug("Error broadcasting SSE event: {}", e.getMessage());
        }
    }

    private void handleDashboard(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestURI().getPath().equals("/") && !exchange.getRequestURI().getPath().equals("/dashboard")) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        byte[] html = DASHBOARD_HTML.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(200, html.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(html);
        }
    }

    private void handleLogo(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        byte[] bytes = null;
        try (var is = getClass().getResourceAsStream("/logo.png")) {
            if (is != null) {
                bytes = is.readAllBytes();
            }
        } catch (Exception ignored) {}

        if (bytes == null) {
            java.nio.file.Path p = java.nio.file.Paths.get("docs/images/logo.png");
            if (java.nio.file.Files.exists(p)) {
                bytes = java.nio.file.Files.readAllBytes(p);
            }
        }

        if (bytes != null) {
            exchange.getResponseHeaders().set("Content-Type", "image/png");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } else {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void addCorsHeaders(HttpExchange exchange) {
        Headers h = exchange.getResponseHeaders();
        h.set("Access-Control-Allow-Origin", "*");
        h.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        h.set("Access-Control-Allow-Headers", "Content-Type, X-Client-Id");
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, Object data) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(data);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonRequest(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return mapper.readValue(is, Map.class);
        }
    }

    @Override
    public void close() {
        running.set(false);
        for (OutputStream os : sseClients) {
            try {
                os.close();
            } catch (IOException ignored) {}
        }
        sseClients.clear();
        server.stop(0);
        log.info("[{}] EmbeddedHttpServer stopped", consensusEngine.nodeId());
    }

    private static final String DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ShardLock Distributed Cluster Monitor</title>
  <style>
    :root {
      --bg: #0b0f19;
      --card-bg: #111827;
      --border: #1f2937;
      --text: #9ca3af;
      --heading: #f3f4f6;
      --accent: #38bdf8;
      --leader: #10b981;
      --follower: #6366f1;
      --candidate: #f59e0b;
      --danger: #ef4444;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 24px; line-height: 1.5; }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--border); }
    h1 { color: var(--heading); font-size: 22px; font-weight: 700; letter-spacing: -0.02em; }
    .status-badge { padding: 4px 14px; border-radius: 9999px; font-weight: 600; font-size: 12px; letter-spacing: 0.05em; text-transform: uppercase; }
    .role-LEADER { background: rgba(16,185,129,0.2); color: #34d399; border: 1px solid #10b981; }
    .role-FOLLOWER { background: rgba(99,102,241,0.2); color: #818cf8; border: 1px solid #6366f1; }
    .role-CANDIDATE, .role-PRE_CANDIDATE { background: rgba(245,158,11,0.2); color: #fbbf24; border: 1px solid #f59e0b; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 20px; margin-bottom: 24px; }
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 20px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.3); }
    .card h2 { color: var(--heading); font-size: 15px; font-weight: 600; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }
    .metric { display: flex; justify-content: space-between; margin-bottom: 10px; font-size: 13px; }
    .metric-value { font-weight: 600; color: var(--accent); font-family: ui-monospace, monospace; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }
    th, td { text-align: left; padding: 10px; border-bottom: 1px solid var(--border); }
    th { color: var(--heading); font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }
    .token-badge { background: #1f2937; border: 1px solid #374151; padding: 2px 8px; border-radius: 6px; font-family: ui-monospace, monospace; color: #f3f4f6; font-size: 12px; }
    .mode-badge { padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 600; }
    .mode-EXCLUSIVE { background: rgba(239,68,68,0.2); color: #f87171; border: 1px solid rgba(239,68,68,0.4); }
    .mode-SHARED { background: rgba(56,189,248,0.2); color: #38bdf8; border: 1px solid rgba(56,189,248,0.4); }
    .progress-bar-bg { width: 100%; height: 5px; background: #1f2937; border-radius: 3px; overflow: hidden; margin-top: 4px; }
    .progress-bar-fill { height: 100%; background: var(--accent); width: 100%; transition: width 0.5s ease; }
    input, select, button { padding: 8px 12px; border-radius: 6px; border: 1px solid var(--border); background: #1f2937; color: #f3f4f6; font-size: 13px; }
    input:focus, select:focus { border-color: var(--accent); outline: none; }
    button { background: #10b981; border-color: #059669; cursor: pointer; font-weight: 600; transition: all 0.2s; }
    button:hover { background: #059669; }
    button.release-btn { background: #ef4444; border-color: #dc2626; padding: 4px 10px; font-size: 12px; }
    button.release-btn:hover { background: #dc2626; }
    .timeline { max-height: 220px; overflow-y: auto; display: flex; flex-direction: column; gap: 8px; font-size: 12px; }
    .timeline-item { padding: 8px 12px; border-radius: 6px; background: #1f2937; border-left: 3px solid var(--accent); display: flex; justify-content: space-between; align-items: center; }
    .timeline-item.LeaderElectedEvent { border-left-color: #10b981; }
    .timeline-item.LockAcquiredEvent { border-left-color: #38bdf8; }
    .timeline-item.LockReleasedEvent { border-left-color: #9ca3af; }
    .timeline-item.LockExpiredEvent { border-left-color: #ef4444; }
  </style>
</head>
<body>
  <header>
    <div style="display: flex; align-items: center; gap: 14px;">
      <img src="/logo.png" width="46" height="46" style="border-radius: 10px; border: 1px solid var(--border); box-shadow: 0 2px 8px rgba(0,0,0,0.4);" alt="ShardLock Sentinel Logo">
      <div>
        <h1>ShardLock Cluster Monitor</h1>
        <p style="font-size: 13px; color: #6b7280; margin-top: 2px;">
          Linearizable Lease Coordinator with Monotonic Fencing Tokens &bull;
          <a href="/metrics" target="_blank" style="color:var(--accent); text-decoration:none;">Prometheus Metrics</a>
        </p>
      </div>
    </div>
    <div id="role-badge" class="status-badge role-FOLLOWER">CONNECTING...</div>
  </header>

  <div class="grid">
    <div class="card">
      <h2>Raft Consensus State</h2>
      <div class="metric"><span>Node ID:</span><span id="metric-node-id" class="metric-value">-</span></div>
      <div class="metric"><span>Current Term:</span><span id="metric-term" class="metric-value">-</span></div>
      <div class="metric"><span>Cluster Leader:</span><span id="metric-leader" class="metric-value">-</span></div>
      <div class="metric"><span>Commit Index:</span><span id="metric-commit" class="metric-value">-</span></div>
      <div class="metric"><span>Fencing Counter:</span><span id="metric-fencing" class="metric-value">-</span></div>
      <div class="metric"><span>Connected Peers:</span><span id="metric-peers" class="metric-value">-</span></div>
    </div>

    <div class="card">
      <h2>Acquire / Wait Queue Request</h2>
      <div style="display:flex; flex-direction:column; gap:8px;">
        <input type="text" id="acq-resource" placeholder="Resource Name" value="partition-0">
        <input type="text" id="acq-client" placeholder="Client ID" value="worker-console">
        <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 8px;">
          <select id="acq-mode">
            <option value="EXCLUSIVE">Exclusive Lock (Write)</option>
            <option value="SHARED">Shared Lock (Read)</option>
          </select>
          <input type="number" id="acq-wait" placeholder="Wait Timeout (ms)" value="0">
        </div>
        <input type="number" id="acq-ttl" placeholder="Lease TTL (ms)" value="10000">
        <button onclick="acquireLock()">Acquire Lease</button>
      </div>
      <div id="action-feedback" style="margin-top:10px; font-size:12px; color: #9ca3af;"></div>
    </div>

    <div class="card">
      <h2>Live SSE Event Stream</h2>
      <div class="timeline" id="event-timeline">
        <div style="text-align:center; color:#6b7280; padding: 20px;">Listening for cluster events...</div>
      </div>
    </div>
  </div>

  <div class="grid">
    <div class="card" style="grid-column: 1 / -1;">
      <h2><span>Active Partition Leases</span> <span class="token-badge" id="lock-count">0 active</span></h2>
      <table>
        <thead>
          <tr>
            <th>Resource</th>
            <th>Owner Client</th>
            <th>Mode</th>
            <th>Fencing Token</th>
            <th>Remaining TTL</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody id="locks-body">
          <tr><td colspan="6" style="text-align:center; color:#6b7280;">No active leases held</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <script>
    const events = [];

    async function updateStatus() {
      try {
        const res = await fetch('/api/v1/cluster/status');
        if (!res.ok) return;
        const data = await res.json();
        document.getElementById('metric-node-id').textContent = data.nodeId;
        document.getElementById('metric-term').textContent = data.currentTerm;
        document.getElementById('metric-leader').textContent = data.leaderId || 'None (Electing)';
        document.getElementById('metric-commit').textContent = data.commitIndex;
        document.getElementById('metric-fencing').textContent = data.fencingCounter;
        document.getElementById('metric-peers').textContent = (data.peers || []).join(', ') || 'Standalone';

        const badge = document.getElementById('role-badge');
        badge.textContent = data.role;
        badge.className = 'status-badge role-' + data.role;
      } catch (e) {
        console.debug('Status update error', e);
      }
    }

    async function updateLocks() {
      try {
        const res = await fetch('/api/v1/locks');
        if (!res.ok) return;
        const locks = await res.json();
        document.getElementById('lock-count').textContent = locks.length + ' Active Leases';
        const tbody = document.getElementById('locks-body');
        if (locks.length === 0) {
          tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; color:#6b7280;">No active leases held</td></tr>';
          return;
        }

        let html = '';
        locks.forEach(l => {
          const pct = Math.min(100, Math.max(0, (l.remainingTtlMs / l.ttlMs) * 100));
          const mode = l.mode || 'EXCLUSIVE';
          html += `<tr>
            <td><strong>${l.resource}</strong></td>
            <td>${l.ownerClientId}</td>
            <td><span class="mode-badge mode-${mode}">${mode}</span></td>
            <td><span class="token-badge">Token #${l.fencingToken}</span></td>
            <td>
              <div>${(l.remainingTtlMs / 1000).toFixed(1)}s remaining</div>
              <div class="progress-bar-bg"><div class="progress-bar-fill" style="width:${pct}%"></div></div>
            </td>
            <td>
              <button class="release-btn" onclick="releaseLock('${l.resource}', '${l.ownerClientId}', ${l.fencingToken})">Release</button>
            </td>
          </tr>`;
        });
        tbody.innerHTML = html;
      } catch (e) {
        console.debug('Locks update error', e);
      }
    }

    async function acquireLock() {
      const resource = document.getElementById('acq-resource').value;
      const clientId = document.getElementById('acq-client').value;
      const mode = document.getElementById('acq-mode').value;
      const waitTimeoutMs = parseInt(document.getElementById('acq-wait').value, 10) || 0;
      const ttlMs = parseInt(document.getElementById('acq-ttl').value, 10) || 10000;
      const feedback = document.getElementById('action-feedback');

      feedback.textContent = waitTimeoutMs > 0 ? 'Waiting in queue...' : 'Acquiring lease...';
      try {
        const res = await fetch('/api/v1/locks/acquire', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ resource, clientId, mode, waitTimeoutMs, ttlMs })
        });
        const data = await res.json();
        if (res.ok) {
          feedback.style.color = '#34d399';
          feedback.textContent = `Granted (${data.mode || 'EXCLUSIVE'})! Fencing Token: #${data.fencingToken}`;
        } else {
          feedback.style.color = '#f87171';
          feedback.textContent = 'Rejected: ' + (data.message || data.error);
        }
        updateLocks();
        updateStatus();
      } catch (e) {
        feedback.style.color = '#f87171';
        feedback.textContent = 'Error: ' + e.message;
      }
    }

    async function releaseLock(resource, clientId, fencingToken) {
      try {
        await fetch('/api/v1/locks/release', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ resource, clientId, fencingToken })
        });
        updateLocks();
        updateStatus();
      } catch (e) {
        alert('Release error: ' + e.message);
      }
    }

    function addTimelineEvent(item) {
      events.unshift(item);
      if (events.length > 20) events.pop();

      const container = document.getElementById('event-timeline');
      container.innerHTML = events.map(e => `
        <div class="timeline-item ${e.eventType}">
          <div>
            <strong>${e.eventType.replace('Event', '')}</strong>
            <span style="color:#9ca3af; margin-left:6px;">${e.detail}</span>
          </div>
          <span style="color:#6b7280; font-family:ui-monospace, monospace;">${new Date(e.timestamp).toLocaleTimeString()}</span>
        </div>
      `).join('');
    }

    function connectSse() {
      const es = new EventSource('/api/v1/cluster/events');
      es.onmessage = (e) => {
        try {
          const ev = JSON.parse(e.data);
          let detail = ev.resource ? `${ev.resource} (#${ev.fencingToken})` : (ev.leaderId || ev.newRole || '');
          addTimelineEvent({ eventType: ev.eventType, detail, timestamp: ev.timestamp || Date.now() });
        } catch (err) {}
        updateStatus();
        updateLocks();
      };
      es.onerror = () => {
        setTimeout(connectSse, 3000);
      };
    }

    updateStatus();
    updateLocks();
    connectSse();
    setInterval(updateStatus, 1500);
    setInterval(updateLocks, 1000);
  </script>
</body>
</html>
""";
}
