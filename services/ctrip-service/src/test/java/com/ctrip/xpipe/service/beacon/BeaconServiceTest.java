package com.ctrip.xpipe.service.beacon;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.service.AbstractServiceTest;
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

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/26
 */
@RunWith(MockitoJUnitRunner.class)
public class BeaconServiceTest extends AbstractServiceTest {

    private BeaconService beaconService;

    private MockWebServer webServer;

    private final String system = "xpipe";
    private final int weight = 20;

    @Before
    public void setupDefaultBeaconServiceTest() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        beaconService = new BeaconService("beacon", "http://127.0.0.1:" + webServer.getPort(), weight);
    }

    @After
    public void afterDefaultBeaconServiceTest() throws Exception {
        webServer.shutdown();
    }

    @Test
    public void testFetchAllClusters() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"code\": 0,\n" +
                "    \"msg\": \"success\",\n" +
                "    \"data\": [\"cluster1\", \"cluster2\"]\n" +
                "}")
                .setHeader("Content-Type", "application/json"));

        Set<String> clusters = beaconService.fetchAllClusters(system);
        Assert.assertEquals(Sets.newHashSet("cluster1", "cluster2"), clusters);

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/v1/monitor/xpipe/clusters", request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test(expected = BeaconServiceException.class)
    public void testFetchAllClustersRespErr() {
        enqueueServerErr();
        beaconService.fetchAllClusters(system);
    }

    @Test
    public void testRegisterCluster() throws Exception {
        Set<MonitorGroupMeta> groups = mockGroups();
        enqueueServerSuccess();
        beaconService.registerCluster(system, "cluster1", groups);

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/v1/monitor/xpipe/cluster/cluster1", request.getPath());
        Assert.assertEquals("POST", request.getMethod());
        MonitorClusterMeta clusterMeta = Codec.DEFAULT.decode(request.getBody().readByteArray(), MonitorClusterMeta.class);
        Assert.assertEquals(groups, clusterMeta.getNodeGroups());
    }

    @Test(expected = BeaconServiceException.class)
    public void testRegisterClusterRespErr() {
        enqueueServerErr();
        beaconService.registerCluster(system, "cluster1", Collections.emptySet());
    }

    @Test
    public void testUnregisterCluster() throws Exception {
        enqueueServerSuccess();
        beaconService.unregisterCluster(system, "cluster1");

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/v1/monitor/xpipe/cluster/cluster1", request.getPath());
        Assert.assertEquals("DELETE", request.getMethod());
    }

    @Test
    public void testUpdateBeaconHost() throws Exception {
        MockWebServer mockWebServer = new MockWebServer();
        mockWebServer.start(InetAddress.getByName("127.0.0.2"), randomPort());
        beaconService.updateHost("http://127.0.0.2:" + mockWebServer.getPort());
        mockWebServer.enqueue(new MockResponse().setBody("{\n" +
                        "    \"code\": 0,\n" +
                        "    \"msg\": \"success\"" +
                        "}")
                .setHeader("Content-Type", "application/json"));
        beaconService.unregisterCluster(system, "cluster1");

        RecordedRequest request = mockWebServer.takeRequest();
        Assert.assertEquals("127.0.0.2", request.getRequestUrl().host());
    }

    @Test(expected = BeaconServiceException.class)
    public void testUnregisterClusterRespErr() {
        enqueueServerErr();
        beaconService.unregisterCluster(system, "cluster1");
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

    Set<MonitorGroupMeta> mockGroups() {
        Set<MonitorGroupMeta> groups = new HashSet<>();
        groups.add(new MonitorGroupMeta("shard1", "oy", Collections.singleton(new HostPort("10.0.0.1", 6379)), true));
        groups.add(new MonitorGroupMeta("shard1", "rb", Collections.singleton(new HostPort("10.0.0.2", 6379)), false));

        return groups;
    }

}
