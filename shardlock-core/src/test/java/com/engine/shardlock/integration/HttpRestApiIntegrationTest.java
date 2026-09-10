package com.engine.shardlock.integration;

import com.engine.shardlock.domain.model.NodeId;
import com.engine.shardlock.infrastructure.server.ShardLockServerNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HttpRestApiIntegrationTest {

    private ShardLockServerNode node;
    private int httpPort;
    private int peerPort;
    private HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();

    private int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        httpPort = findFreePort();
        peerPort = findFreePort();

        node = new ShardLockServerNode(
                NodeId.of("node-test"),
                httpPort,
                peerPort,
                Map.of(),
                Map.of(),
                tempDir
        );
        node.start();

        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        // Wait briefly for single node election to complete
        long deadline = System.currentTimeMillis() + 3000;
        while (node.consensusEngine().role() != com.engine.shardlock.domain.model.NodeRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.stop();
        }
    }

    @Test
    @DisplayName("Cluster status endpoint returns node metrics")
    void testGetClusterStatus() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + httpPort + "/api/v1/cluster/status"))
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);

        Map<String, Object> data = mapper.readValue(resp.body(), Map.class);
        assertThat(data.get("nodeId")).isEqualTo("node-test");
        assertThat(data.get("role")).isEqualTo("LEADER");
    }

    @Test
    @DisplayName("Acquires, renews, lists, and releases lock via HTTP API")
    void testLockLifecycleViaHttp() throws Exception {
        String base = "http://127.0.0.1:" + httpPort;

        // 1. Acquire Lock
        String acqPayload = mapper.writeValueAsString(Map.of(
                "resource", "partition-0",
                "clientId", "worker-1",
                "ttlMs", 10000
        ));
        HttpRequest acqReq = HttpRequest.newBuilder()
                .uri(URI.create(base + "/api/v1/locks/acquire"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(acqPayload))
                .build();

        HttpResponse<String> acqResp = httpClient.send(acqReq, HttpResponse.BodyHandlers.ofString());
        assertThat(acqResp.statusCode()).isEqualTo(200);
        Map<String, Object> acqData = mapper.readValue(acqResp.body(), Map.class);
        assertThat(acqData.get("status")).isEqualTo("ACQUIRED");
        long fencingToken = ((Number) acqData.get("fencingToken")).longValue();
        assertThat(fencingToken).isGreaterThanOrEqualTo(1);

        // 2. List Locks
        HttpRequest listReq = HttpRequest.newBuilder()
                .uri(URI.create(base + "/api/v1/locks"))
                .GET()
                .build();
        HttpResponse<String> listResp = httpClient.send(listReq, HttpResponse.BodyHandlers.ofString());
        assertThat(listResp.statusCode()).isEqualTo(200);
        assertThat(listResp.body()).contains("partition-0");

        // 3. Renew Lock
        String renewPayload = mapper.writeValueAsString(Map.of(
                "resource", "partition-0",
                "clientId", "worker-1",
                "fencingToken", fencingToken,
                "ttlMs", 15000
        ));
        HttpRequest renewReq = HttpRequest.newBuilder()
                .uri(URI.create(base + "/api/v1/locks/renew"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(renewPayload))
                .build();
        HttpResponse<String> renewResp = httpClient.send(renewReq, HttpResponse.BodyHandlers.ofString());
        assertThat(renewResp.statusCode()).isEqualTo(200);

        // 4. Release Lock
        String relPayload = mapper.writeValueAsString(Map.of(
                "resource", "partition-0",
                "clientId", "worker-1",
                "fencingToken", fencingToken
        ));
        HttpRequest relReq = HttpRequest.newBuilder()
                .uri(URI.create(base + "/api/v1/locks/release"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(relPayload))
                .build();
        HttpResponse<String> relResp = httpClient.send(relReq, HttpResponse.BodyHandlers.ofString());
        assertThat(relResp.statusCode()).isEqualTo(200);
    }

    @Test
    @DisplayName("Dashboard UI HTML is served at root path")
    void testDashboardHtmlServed() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + httpPort + "/"))
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.headers().firstValue("Content-Type")).isPresent();
        assertThat(resp.headers().firstValue("Content-Type").get()).contains("text/html");
        assertThat(resp.body()).contains("ShardLock");
    }

    @Test
    @DisplayName("Prometheus /metrics endpoint exposes standard gauge metrics")
    void testMetricsEndpoint() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + httpPort + "/metrics"))
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("shardlock_raft_term");
        assertThat(resp.body()).contains("shardlock_raft_is_leader");
        assertThat(resp.body()).contains("shardlock_fencing_token_current");
    }

    @Test
    @DisplayName("Shared lock and wait queue endpoints respond correctly")
    void testSharedLockAndWaitQueueViaHttp() throws Exception {
        String base = "http://127.0.0.1:" + httpPort;

        // 1. Acquire SHARED lock
        String acqPayload = mapper.writeValueAsString(Map.of(
                "resource", "shared-partition",
                "clientId", "reader-1",
                "mode", "SHARED",
                "ttlMs", 10000
        ));
        HttpRequest acqReq = HttpRequest.newBuilder()
                .uri(URI.create(base + "/api/v1/locks/acquire"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(acqPayload))
                .build();

        HttpResponse<String> acqResp = httpClient.send(acqReq, HttpResponse.BodyHandlers.ofString());
        assertThat(acqResp.statusCode()).isEqualTo(200);
        Map<String, Object> data = mapper.readValue(acqResp.body(), Map.class);
        assertThat(data.get("mode")).isEqualTo("SHARED");

        // 2. Query Wait Queue
        HttpRequest waitReq = HttpRequest.newBuilder()
                .uri(URI.create(base + "/api/v1/locks/waiters"))
                .GET()
                .build();
        HttpResponse<String> waitResp = httpClient.send(waitReq, HttpResponse.BodyHandlers.ofString());
        assertThat(waitResp.statusCode()).isEqualTo(200);
        assertThat(waitResp.body()).contains("queueLengths");
    }
}
