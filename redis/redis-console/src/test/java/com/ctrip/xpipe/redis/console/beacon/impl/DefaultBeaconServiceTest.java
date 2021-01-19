package com.ctrip.xpipe.redis.console.beacon.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.beacon.data.BeaconClusterMeta;
import com.ctrip.xpipe.redis.console.beacon.data.BeaconGroupMeta;
import com.ctrip.xpipe.redis.console.beacon.exception.BeaconServiceException;
import com.google.common.collect.Sets;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.ctrip.xpipe.redis.console.beacon.impl.DefaultBeaconService.PATH_GET_CLUSTERS;

/**
 * @author lishanglin
 * date 2021/1/18
 */
public class DefaultBeaconServiceTest extends AbstractTest {

    private DefaultBeaconService beaconService;

    private MockWebServer webServer;

    @Before
    public void setupDefaultBeaconServiceTest() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        beaconService = new DefaultBeaconService("http://127.0.0.1:" + webServer.getPort());
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

        Set<String> clusters = beaconService.fetchAllClusters();
        Assert.assertEquals(Sets.newHashSet("cluster1", "cluster2"), clusters);

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals(PATH_GET_CLUSTERS, request.getPath());
        Assert.assertEquals("GET", request.getMethod());
    }

    @Test(expected = BeaconServiceException.class)
    public void testFetchAllClustersRespErr() {
        enqueueServerErr();
        beaconService.fetchAllClusters();
    }

    @Test
    public void testRegisterCluster() throws Exception {
        Set<BeaconGroupMeta> groups = mockGroups();
        enqueueServerSuccess();
        beaconService.registerCluster("cluster1", groups);

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/v1/monitor/xpipe/cluster1", request.getPath());
        Assert.assertEquals("POST", request.getMethod());
        BeaconClusterMeta clusterMeta = Codec.DEFAULT.decode(request.getBody().readByteArray(), BeaconClusterMeta.class);
        Assert.assertEquals(groups, clusterMeta.getNodeGroups());
    }

    @Test(expected = BeaconServiceException.class)
    public void testRegisterClusterRespErr() {
        enqueueServerErr();
        beaconService.registerCluster("cluster1", Collections.emptySet());
    }

    @Test
    public void testUnregisterCluster() throws Exception {
        enqueueServerSuccess();
        beaconService.unregisterCluster("cluster1");

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/api/v1/monitor/xpipe/cluster1", request.getPath());
        Assert.assertEquals("DELETE", request.getMethod());
    }

    @Test(expected = BeaconServiceException.class)
    public void testUnregisterClusterRespErr() {
        enqueueServerErr();
        beaconService.unregisterCluster("cluster1");
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

    Set<BeaconGroupMeta> mockGroups() {
        Set<BeaconGroupMeta> groups = new HashSet<>();
        groups.add(new BeaconGroupMeta("shard1", "oy", Collections.singleton(new HostPort("10.0.0.1", 6379)), true));
        groups.add(new BeaconGroupMeta("shard1", "rb", Collections.singleton(new HostPort("10.0.0.2", 6379)), false));

        return groups;
    }

}
