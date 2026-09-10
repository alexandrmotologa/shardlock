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
        server.createContext("/api/v1/locks", this::handleListLocks);
        server.createContext("/api/v1/cluster/events", this::handleSseEvents);
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

        sendJsonResponse(exchange, 200, status);
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

            if (resource == null || resource.isBlank() || clientId == null || clientId.isBlank()) {
                sendJsonResponse(exchange, 400, Map.of("error", "resource and clientId are required"));
                return;
            }

            StateMachineResult res = lockManager.acquireLock(resource, clientId, Duration.ofMillis(ttlMs)).get(5, TimeUnit.SECONDS);

            if (res.isSuccess()) {
                LockRecord rec = res.lockRecord();
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("status", "ACQUIRED");
                resp.put("resource", rec.resource());
                resp.put("clientId", rec.ownerClientId());
                resp.put("fencingToken", rec.fencingToken().value());
                resp.put("expiresAtMs", rec.expiresAtMs());
                resp.put("ttlMs", rec.ttlMs());
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
        os.flush();
    }

    public synchronized void publishEvent(RaftDomainEvent event) {
        if (sseClients.isEmpty()) {
            return;
        }

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
  <title>ShardLock Cluster Monitor</title>
  <style>
    :root {
      --bg: #0d1117;
      --card-bg: #161b22;
      --border: #30363d;
      --text: #c9d1d9;
      --heading: #f0f6fc;
      --accent: #58a6ff;
      --leader: #238636;
      --follower: #1f6feb;
      --candidate: #d29922;
      --danger: #da3633;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 24px; }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--border); }
    h1 { color: var(--heading); font-size: 24px; }
    .status-badge { padding: 4px 12px; border-radius: 12px; font-weight: 600; font-size: 13px; text-transform: uppercase; }
    .role-LEADER { background: var(--leader); color: #fff; }
    .role-FOLLOWER { background: var(--follower); color: #fff; }
    .role-CANDIDATE { background: var(--candidate); color: #fff; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 20px; margin-bottom: 24px; }
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; padding: 18px; }
    .card h2 { color: var(--heading); font-size: 16px; margin-bottom: 14px; }
    .metric { display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 14px; }
    .metric-value { font-weight: 600; color: var(--accent); }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }
    th, td { text-align: left; padding: 10px; border-bottom: 1px solid var(--border); }
    th { color: var(--heading); font-weight: 600; }
    .token-badge { background: #21262d; border: 1px solid var(--border); padding: 2px 8px; border-radius: 4px; font-family: monospace; }
    .progress-bar-bg { width: 100%; height: 6px; background: #21262d; border-radius: 3px; overflow: hidden; margin-top: 4px; }
    .progress-bar-fill { height: 100%; background: var(--accent); width: 100%; transition: width 0.5s ease; }
    .action-row { display: flex; gap: 10px; margin-top: 14px; }
    input, button { padding: 8px 12px; border-radius: 6px; border: 1px solid var(--border); background: #21262d; color: #fff; font-size: 14px; }
    button { background: #238636; border-color: #2ea043; cursor: pointer; font-weight: 600; }
    button:hover { background: #2ea043; }
    button.release-btn { background: var(--danger); border-color: #f85149; }
    .pulse { animation: pulse-anim 1.5s infinite; }
    @keyframes pulse-anim { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>ShardLock Consensus Daemon</h1>
      <p style="font-size: 13px; color: #8b949e; margin-top: 4px;">Partition Lease Coordinator &amp; Monotonic Fencing Engine</p>
    </div>
    <div id="role-badge" class="status-badge role-FOLLOWER">INITIALIZING</div>
  </header>

  <div class="grid">
    <div class="card">
      <h2>Cluster Consensus</h2>
      <div class="metric"><span>Node ID:</span><span id="metric-node-id" class="metric-value">-</span></div>
      <div class="metric"><span>Current Term:</span><span id="metric-term" class="metric-value">-</span></div>
      <div class="metric"><span>Recognized Leader:</span><span id="metric-leader" class="metric-value">-</span></div>
      <div class="metric"><span>Commit Index:</span><span id="metric-commit" class="metric-value">-</span></div>
      <div class="metric"><span>Fencing Token Counter:</span><span id="metric-fencing" class="metric-value">-</span></div>
    </div>

    <div class="card">
      <h2>Acquire Resource Lock</h2>
      <div style="display:flex; flex-direction:column; gap:8px;">
        <input type="text" id="acq-resource" placeholder="Resource Name (e.g. partition-0)" value="orders-partition-0">
        <input type="text" id="acq-client" placeholder="Client ID" value="worker-console">
        <input type="number" id="acq-ttl" placeholder="TTL in Milliseconds" value="10000">
        <button onclick="acquireLock()">Acquire Lease</button>
      </div>
      <div id="action-feedback" style="margin-top:10px; font-size:13px; color: #8b949e;"></div>
    </div>
  </div>

  <div class="card">
    <h2>Active Partition Leases (<span id="lock-count">0</span>)</h2>
    <table>
      <thead>
        <tr>
          <th>Resource</th>
          <th>Owner Client</th>
          <th>Fencing Token</th>
          <th>Remaining TTL</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody id="locks-body">
        <tr><td colspan="5" style="text-align:center; color:#8b949e;">No active leases held</td></tr>
      </tbody>
    </table>
  </div>

  <script>
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
        document.getElementById('lock-count').textContent = locks.length;
        const tbody = document.getElementById('locks-body');
        if (locks.length === 0) {
          tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color:#8b949e;">No active leases held</td></tr>';
          return;
        }

        let html = '';
        locks.forEach(l => {
          const pct = Math.min(100, Math.max(0, (l.remainingTtlMs / l.ttlMs) * 100));
          html += `<tr>
            <td><strong>${l.resource}</strong></td>
            <td>${l.ownerClientId}</td>
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
      const ttlMs = parseInt(document.getElementById('acq-ttl').value, 10);
      const feedback = document.getElementById('action-feedback');

      feedback.textContent = 'Acquiring lock...';
      try {
        const res = await fetch('/api/v1/locks/acquire', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ resource, clientId, ttlMs })
        });
        const data = await res.json();
        if (res.ok) {
          feedback.style.color = '#3fb950';
          feedback.textContent = 'Lock Acquired! Fencing Token: ' + data.fencingToken;
        } else {
          feedback.style.color = '#f85149';
          feedback.textContent = 'Failed: ' + (data.message || data.error);
        }
        updateLocks();
        updateStatus();
      } catch (e) {
        feedback.style.color = '#f85149';
        feedback.textContent = 'Network error: ' + e.message;
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

    // SSE Event Listener for instant push updates
    function connectSse() {
      const es = new EventSource('/api/v1/cluster/events');
      es.onmessage = (e) => {
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
    setInterval(updateStatus, 1000);
    setInterval(updateLocks, 1000);
  </script>
</body>
</html>
""";
}
