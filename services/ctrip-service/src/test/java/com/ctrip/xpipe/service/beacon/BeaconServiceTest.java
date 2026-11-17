package com.ctrip.xpipe.service.beacon;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MockMetricProxy;
import com.ctrip.xpipe.service.AbstractServiceTest;
import com.ctrip.xpipe.service.beacon.data.BeaconResp;
import com.ctrip.xpipe.service.beacon.exception.BeaconServiceException;
import com.google.common.collect.Sets;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2021/1/26
 */
@RunWith(MockitoJUnitRunner.class)
public class BeaconServiceTest extends AbstractServiceTest {

    private BeaconService beaconService;

    private MockWebServer webServer;

    private MockMetricProxy metricProxy = new MockMetricProxy();

    private final String system = "xpipe";
    private final String cluster = "cluster1";
    private final int weight = 20;

    @Before
    public void setupDefaultBeaconServiceTest() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        beaconService = new BeaconService("beacon", "http://127.0.0.1:" + webServer.getPort(), weight);
        metricProxy.reset();
        beaconService.setMetricProxy(metricProxy);
    }

    @After
    public void afterDefaultBeaconServiceTest() throws Exception {
        beaconService.setMetricProxy(MetricProxy.DEFAULT);
        webServer.shutdown();
    }

    @Test
    public void testFetchAllClusters() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"code\": 0,\n" +
                "    \"msg\": \"success\",\n" +
                "    \"data\": [\"cluster1\", \"cluster2\"]\n" +
                "}")
                .setBodyDelay(50, TimeUnit.MILLISECONDS)
                .setHeader("Content-Type", "application/json"));

        Set<String> clusters = beaconService.fetchAllClusters(system);
        Assert.assertEquals(Sets.newHashSet("cluster1", "cluster2"), clusters);

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/v1/monitor/xpipe/clusters", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
        MetricData metricData = assertBeaconMetric("fetchAllClusters", "GET", true, system, "-");
        Assert.assertTrue(metricData.getValue() >= 50);
    }

    @Test
    public void testFetchAllClustersNullDataMetric() {
        webServer.enqueue(new MockResponse().setBody("{\"code\":0,\"msg\":\"success\"}")
                .setHeader("Content-Type", "application/json"));

        Assert.assertNull(beaconService.fetchAllClusters(system));

        assertBeaconMetric("fetchAllClusters", "GET", true, system, "-");
    }

    @Test
    public void testFetchAllClustersRespErr() {
        enqueueServerErr();

        assertBeaconServiceException(() -> beaconService.fetchAllClusters(system));

        assertBeaconMetric("fetchAllClusters", "GET", false, system, "-");
    }

    @Test
    public void testFetchAllClustersHttpErrorMetric() {
        webServer.enqueue(new MockResponse().setResponseCode(500));

        try {
            beaconService.fetchAllClusters(system);
            Assert.fail("expected HttpServerErrorException");
        } catch (HttpServerErrorException ignored) {
        }

        assertBeaconMetric("fetchAllClusters", "GET", false, system, "-");
    }

    @Test
    public void testRegisterCluster() throws Exception {
        Set<MonitorGroupMeta> groups = mockGroups();
        Map<String, String> extraData = mockExtra();
        enqueueServerSuccess();
        beaconService.registerCluster(system, cluster, groups, extraData);

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/v1/monitor/xpipe/cluster/cluster1", request.getPath());
        Assert.assertEquals("POST", request.getMethod());
        MonitorClusterMeta clusterMeta = Codec.DEFAULT.decode(request.getBody().readByteArray(), MonitorClusterMeta.class);
        Assert.assertEquals(groups, clusterMeta.getNodeGroups());
        Assert.assertEquals(extraData, clusterMeta.getExtra());
        assertBeaconMetric("registerCluster", "POST", true, system, cluster);
    }

    @Test
    public void testRegisterClusterRespErr() {
        enqueueServerErr();

        assertBeaconServiceException(() -> beaconService.registerCluster(system, cluster, Collections.emptySet(), mockExtra()));

        assertBeaconMetric("registerCluster", "POST", false, system, cluster);
    }

    @Test
    public void testUnregisterCluster() throws Exception {
        enqueueServerSuccess();
        beaconService.unregisterCluster(system, cluster);

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/v1/monitor/xpipe/cluster/cluster1", request.getPath());
        Assert.assertEquals("DELETE", request.getMethod());
        assertBeaconMetric("unregisterCluster", "DELETE", true, system, cluster);
    }

    @Test
    public void testUnregisterClusterRespErr() {
        enqueueServerErr();

        assertBeaconServiceException(() -> beaconService.unregisterCluster(system, cluster));

        assertBeaconMetric("unregisterCluster", "DELETE", false, system, cluster);
    }

    @Test
    public void testUnregisterClusterWithoutRequestDoesNotMetric() {
        beaconService.unregisterCluster(null, cluster);
        beaconService.unregisterCluster(system, null);

        Assert.assertEquals(0, webServer.getRequestCount());
        Assert.assertNull(metricProxy.poll());
    }

    @Test
    public void testUpdateClusterDoesNotMetric() throws Exception {
        enqueueServerSuccess();

        beaconService.updateCluster(system, cluster, Collections.emptySet(), mockExtra());

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("PUT", request.getMethod());
        Assert.assertNull(metricProxy.poll());
    }

    @Test
    public void testUpdateBeaconHost() throws Exception {
        try (MockWebServer mockWebServer = new MockWebServer()) {
            mockWebServer.start(InetAddress.getByName("localhost"), randomPort());
            beaconService.updateHost("http://localhost:" + mockWebServer.getPort());
            mockWebServer.enqueue(new MockResponse().setBody("{\n" +
                            "    \"code\": 0,\n" +
                            "    \"msg\": \"success\"" +
                            "}")
                    .setHeader("Content-Type", "application/json"));
            beaconService.unregisterCluster(system, cluster);

            RecordedRequest request = mockWebServer.takeRequest();
            Assert.assertEquals("localhost", request.getRequestUrl().host());
            assertBeaconMetric("unregisterCluster", "DELETE", true, system, cluster);
        }
    }

    @Test
    public void testGetAllClusterWithDc() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                        "    \"code\": 0,\n" +
                        "    \"msg\": \"success\",\n" +
                        "    \"data\": {\n" +
                        "      \"xpipe_dr_pt_cluster_2271\": [\n" +
                        "        \"PTOY\"\n" +
                        "      ],\n" +
                        "      \"xpipe_dr_pt_cluster_210\": [\n" +
                        "        \"PTOY\"\n" +
                        "      ],\n" +
                        "      \"xpipe_dr_pt_cluster_453\": [\n" +
                        "        \"PTOY\"\n" +
                        "      ],\n" +
                        "      \"xpipe_dr_pt_cluster_2270\": [\n" +
                        "        \"PTOY\"\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  }")
                .setHeader("Content-Type", "application/json"));
        Map<String, Set<String>> data = beaconService.getAllClusterWithDc(system);
        Set<String> remote = data.get("xpipe_dr_pt_cluster_2271");
        Set<String> local = new HashSet<>();
        local.add("PTOY");
        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals(remote, local);
        Assert.assertEquals("GET", request.getMethod());
        assertBeaconMetric("getAllClusterWithDc", "GET", true, system, "-");
    }

    @Test
    public void testGetBeaconClusterHash() throws Exception {
        String hashCluster = "xpipe_dr_pt_cluster_2247";
        webServer.enqueue(new MockResponse().setBody("{\"code\":0,\"msg\":\"success\",\"data\":44773398}")
                .setHeader("Content-Type", "application/json"));
        int hash = beaconService.getBeaconClusterHash(system, hashCluster);
        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals(44773398, hash);
        Assert.assertEquals("GET", request.getMethod());
        assertBeaconMetric("getBeaconClusterHash", "GET", true, system, hashCluster);
    }

    @Test
    public void testGetClusterExtraDoesNotMetric() throws Exception {
        Map<String, String> extra = mockExtra();
        webServer.enqueue(new MockResponse()
                .setBody(Codec.DEFAULT.encode(new BeaconResp<>(extra)))
                .setHeader("Content-Type", "application/json"));
        Map<String, String> resp = beaconService.getBeaconClusterExtra(system, cluster);
        Assert.assertEquals(extra, resp);
        Assert.assertNull(metricProxy.poll());
    }

    @Test(expected = HttpClientErrorException.NotFound.class)
    public void testGetClusterExtra404() throws Exception {
        webServer.enqueue(new MockResponse()
                .setResponseCode(404).setBody("<html><body><h1>Whitelabel Error Page</h1></body></html>"));
        beaconService.getBeaconClusterExtra(system, cluster);
    }

    private MetricData assertBeaconMetric(String operation, String method, boolean success, String metricSystem, String metricCluster) {
        MetricData metricData = metricProxy.poll();

        Assert.assertNotNull(metricData);
        Assert.assertEquals("call.beacon", metricData.getMetricType());
        Assert.assertTrue(metricData.getValue() >= 0);
        Assert.assertEquals(metricCluster, metricData.getClusterName());
        Assert.assertEquals(operation, metricData.getTags().get("operation"));
        Assert.assertEquals(method, metricData.getTags().get("method"));
        Assert.assertEquals(String.valueOf(success), metricData.getTags().get("success"));
        Assert.assertEquals(metricSystem, metricData.getTags().get("system"));
        Assert.assertNull(metricProxy.poll());
        return metricData;
    }

    private void assertBeaconServiceException(Runnable action) {
        try {
            action.run();
            Assert.fail("expected BeaconServiceException");
        } catch (BeaconServiceException ignored) {
        }
    }

    private void enqueueServerSuccess() {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"code\": 0,\n" +
                "    \"msg\": \"success\"" +
                "}")
                .setHeader("Content-Type", "application/json"));
    }

    private void enqueueServerErr() {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"code\": -1,\n" +
                "    \"msg\": \"success\"" +
                "}")
                .setHeader("Content-Type", "application/json"));
    }

    private Set<MonitorGroupMeta> mockGroups() {
        Set<MonitorGroupMeta> groups = new HashSet<>();
        groups.add(new MonitorGroupMeta("shard1", "oy", Collections.singleton(new HostPort("10.0.0.1", 6379)), true));
        groups.add(new MonitorGroupMeta("shard1", "rb", Collections.singleton(new HostPort("10.0.0.2", 6379)), false));

        return groups;
    }

    private Map<String, String> mockExtra() {
        return Collections.singletonMap("extra", System.currentTimeMillis() + "");
    }
}
