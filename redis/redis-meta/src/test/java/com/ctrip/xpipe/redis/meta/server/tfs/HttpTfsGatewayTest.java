package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpTfsGatewayTest {

    private MockWebServer webServer;
    private UnitTestServerConfig config;
    private HttpTfsGateway gateway;
    private RecordingMetricProxy metricProxy;

    @Before
    public void setUp() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), 0);
        config = new UnitTestServerConfig()
                .setTfsGatewayHost(baseUrl(webServer))
                .setTfsGatewayAppId(100004376L);
        gateway = new HttpTfsGateway(config);
        metricProxy = new RecordingMetricProxy();
        gateway.setMetricProxy(metricProxy);
    }

    @After
    public void tearDown() throws Exception {
        if (webServer != null) {
            webServer.close();
        }
    }

    @Test
    public void testForceCloseDirSuccessPostsJson() throws Exception {
        enqueueStatus(webServer, 0, "ok");

        Assert.assertTrue(gateway.forceCloseDir("fs-1", "/opt/data/repl_9", "10.0.0.1"));

        RecordedRequest request = webServer.takeRequest(1, TimeUnit.SECONDS);
        Assert.assertNotNull(request);
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("/tfs/app/100004376/fs/fs-1/TFSGateway/ForceCloseDir", request.getPath());
        Assert.assertTrue(request.getHeader("Content-Type").contains(HttpTfsGateway.CONTENT_TYPE_JSON));
        assertRequestBody(request, "fs-1", "/opt/data/repl_9", "10.0.0.1");
        assertMetric("SUCCESS");
    }

    @Test
    public void testForceCloseDirNonZeroErrorCodeReturnsFalse() throws Exception {
        enqueueStatus(webServer, 7, "busy");
        Assert.assertFalse(gateway.forceCloseDir("fs-1", "/path", "10.0.0.1"));
        assertMetric("FAIL");
    }

    @Test
    public void testForceCloseDirWithoutStatusReturnsFalse() throws Exception {
        webServer.enqueue(new MockResponse()
                .setHeader("Content-Type", HttpTfsGateway.CONTENT_TYPE_JSON)
                .setBody("{}"));

        Assert.assertFalse(gateway.forceCloseDir("fs-1", "/path", "10.0.0.1"));
        assertMetric("FAIL");
    }

    @Test
    public void testForceCloseDirHttpFailureThrows() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(500));
        try {
            gateway.forceCloseDir("fs-1", "/path", "10.0.0.1");
            Assert.fail("expected exception");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }
        assertMetric("FAIL");
    }

    @Test
    public void testForceCloseDirTimeoutThrows() throws Exception {
        RestOperations shortRest = RestTemplateFactory.createCommonsHttpRestTemplate(
                10, 100, 200, 200, 0);
        HttpTfsGateway shortGateway = new HttpTfsGateway(config, shortRest);
        shortGateway.setMetricProxy(metricProxy);
        webServer.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        try {
            shortGateway.forceCloseDir("fs-1", "/path", "10.0.0.1");
            Assert.fail("expected timeout");
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }
        assertMetric("FAIL");
    }

    @Test
    public void testHostAndAppIdHotReloadOnNextCall() throws Exception {
        enqueueStatus(webServer, 0, "ok");
        Assert.assertTrue(gateway.forceCloseDir("fs-a", "/p1", "10.0.0.1"));
        RecordedRequest first = webServer.takeRequest(1, TimeUnit.SECONDS);
        Assert.assertNotNull(first);
        Assert.assertEquals("/tfs/app/100004376/fs/fs-a/TFSGateway/ForceCloseDir", first.getPath());

        MockWebServer hostB = new MockWebServer();
        hostB.start(InetAddress.getByName("127.0.0.1"), 0);
        try {
            enqueueStatus(hostB, 0, "ok");
            config.setTfsGatewayHost(baseUrl(hostB)).setTfsGatewayAppId(42L);

            Assert.assertTrue(gateway.forceCloseDir("fs-b", "/p2", "10.0.0.2"));

            RecordedRequest second = hostB.takeRequest(1, TimeUnit.SECONDS);
            Assert.assertNotNull(second);
            Assert.assertEquals("/tfs/app/42/fs/fs-b/TFSGateway/ForceCloseDir", second.getPath());
            assertRequestBody(second, "fs-b", "/p2", "10.0.0.2");
        } finally {
            hostB.close();
        }
    }

    @Test
    public void testFactoryCreateReturnsHttpGatewayHoldingConfig() {
        TfsGateway created = TfsGatewayFactory.create(config);
        Assert.assertTrue(created instanceof HttpTfsGateway);
        Assert.assertSame(config, ((HttpTfsGateway) created).getConfig());
    }

    private void assertMetric(String status) {
        Assert.assertFalse(metricProxy.data.isEmpty());
        MetricData last = metricProxy.data.get(metricProxy.data.size() - 1);
        Assert.assertEquals(HttpTfsGateway.METRIC_TYPE, last.getMetricType());
        Assert.assertEquals(HttpTfsGateway.API_FORCE_CLOSE_DIR, last.getTags().get("api"));
        Assert.assertEquals(status, last.getTags().get("status"));
    }

    private static String baseUrl(MockWebServer server) {
        return server.url("/").toString().replaceAll("/$", "");
    }

    private static void assertRequestBody(RecordedRequest request, String fsId, String dirPath, String podIp) {
        Map<?, ?> body = JsonCodec.INSTANCE.decode(request.getBody().readUtf8(), Map.class);
        Assert.assertEquals(fsId, body.get("fs_id"));
        Assert.assertEquals(dirPath, body.get("dir_path"));
        Assert.assertEquals(podIp, body.get("pod_ip"));
    }

    private static void enqueueStatus(MockWebServer server, int errorCode, String message) {
        String body = String.format("{\"status\":{\"error_code\":%d,\"message\":\"%s\"}}", errorCode, message);
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", HttpTfsGateway.CONTENT_TYPE_JSON)
                .setBody(body));
    }

    private static class RecordingMetricProxy implements MetricProxy {
        private final List<MetricData> data = new ArrayList<>();

        @Override
        public void writeBinMultiDataPoint(MetricData point) {
            data.add(point);
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }
}
