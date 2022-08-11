package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.service.AbstractServiceTest;
import com.ctrip.xpipe.metric.MockMetricProxy;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/4/30
 */
@RunWith(MockitoJUnitRunner.class)
public class CRedisServiceHttpTest extends AbstractServiceTest {

    @Mock
    private Config config;

    private MockWebServer webServer;

    private CRedisService credisService = (CRedisService) OuterClientService.DEFAULT;

    private MockMetricProxy metricProxy = new MockMetricProxy();

    @Before
    public void setupCRedisServiceHttpTest() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        when(config.get(matches(CRedisConfig.KEY_CREDIS_SERVEICE_ADDRESS), anyString())).thenReturn("127.0.0.1:" + webServer.getPort());
        when(config.get(matches(CRedisConfig.KEY_CREDIS_IDC_MAPPING_RULE), anyString())).thenReturn("{}");
        CRedisConfig.INSTANCE.setConfig(config);
        metricProxy.reset();
        credisService.setMetricProxy(metricProxy);
    }

    @After
    public void afterCRedisServiceHttpTest() throws Exception {
        credisService.setMetricProxy(MetricProxy.DEFAULT);
        webServer.shutdown();
    }

    @Test
    public void testMarkInstanceDown() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"success\": true,\n" +
                "    \"message\": \"success\"" +
                "}")
                .setHeader("Content-Type", "application/json"));

        ClusterShardHostPort clusterShardHostPort = new ClusterShardHostPort("test-cluster", "shard1",
                new HostPort("10.0.0.1", 6379));
        credisService.markInstanceDown(clusterShardHostPort);

        Assert.assertEquals(1, webServer.getRequestCount());

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/keeperApi/switchReadStatus?clusterName=test-cluster&ip=10.0.0.1&port=6379&canRead=false",
                request.getPath());
        Assert.assertEquals("POST", request.getMethod());

        MetricData metricData = metricProxy.poll();
        Assert.assertNotNull(metricData);
        Assert.assertEquals("call.credis", metricData.getMetricType());
        Assert.assertEquals("markInstanceDown", metricData.getTags().get("api"));
        Assert.assertEquals("test-cluster", metricData.getClusterName());
        Assert.assertEquals("SUCCESS", metricData.getTags().get("status"));
        Assert.assertNull(metricProxy.poll());
    }

    @Test
    public void testMigrationPreCheck() throws Exception {
        webServer.enqueue(new MockResponse().setBody("true").setHeader("Content-Type", "application/json"));

        Assert.assertTrue(credisService.clusterMigratePreCheck("test-cluster"));
        Assert.assertEquals(1, webServer.getRequestCount());

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/keeperApi/checkcluster/test-cluster",
                request.getPath());
        Assert.assertEquals("POST", request.getMethod());

        MetricData metricData = metricProxy.poll();
        Assert.assertNotNull(metricData);
        Assert.assertEquals("call.credis", metricData.getMetricType());
        Assert.assertEquals("clusterMigratePreCheck", metricData.getTags().get("api"));
        Assert.assertEquals("test-cluster", metricData.getClusterName());
        Assert.assertEquals("SUCCESS", metricData.getTags().get("status"));
        Assert.assertNull(metricProxy.poll());
    }

    @Test
    public void testExcludeIdcs() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"success\": true,\n" +
                "    \"message\": \"success\"" +
                "}")
                .setHeader("Content-Type", "application/json"));

        Assert.assertTrue(credisService.excludeIdcs("test-cluster", new String[]{}));
        Assert.assertEquals(1, webServer.getRequestCount());

        RecordedRequest request = webServer.takeRequest();
        Assert.assertEquals("/keeperApi/excludedIdcs/test-cluster",
                request.getPath());
        Assert.assertEquals("POST", request.getMethod());

        MetricData metricData = metricProxy.poll();
        Assert.assertNotNull(metricData);
        Assert.assertEquals("call.credis", metricData.getMetricType());
        Assert.assertEquals("excludeClusterDc", metricData.getTags().get("api"));
        Assert.assertEquals("test-cluster", metricData.getClusterName());
        Assert.assertEquals("SUCCESS", metricData.getTags().get("status"));
        Assert.assertNull(metricProxy.poll());
    }

    @Test
    public void testMarkInstanceDownIfNoModifyFor() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"success\": true,\n" +
                "    \"message\": \"success\"" +
                "}")
                .setHeader("Content-Type", "application/json"));

        ClusterShardHostPort clusterShardHostPort = new ClusterShardHostPort("cluster1", "shard1",
                new HostPort("10.0.0.1", 6379));
        credisService.markInstanceDownIfNoModifyFor(clusterShardHostPort, 60);

        Assert.assertEquals(1, webServer.getRequestCount());
        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals("/keeperApi/switchReadStatus?clusterName=cluster1&ip=10.0.0.1&port=6379&canRead=false&noModifySeconds=60",
                req.getPath());
        Assert.assertEquals("POST", req.getMethod());

        MetricData metricData = metricProxy.poll();
        Assert.assertNotNull(metricData);
        Assert.assertEquals("call.credis", metricData.getMetricType());
        Assert.assertEquals("markInstanceDownIfNoModify", metricData.getTags().get("api"));
        Assert.assertEquals("cluster1", metricData.getClusterName());
        Assert.assertEquals("SUCCESS", metricData.getTags().get("status"));
        Assert.assertNull(metricProxy.poll());
    }

    @Test
    public void testGetActiveDcClusters() throws Exception {
        OuterClientService.ClusterInfo cluster1 = mockClusterInfo("cluster1", "jq", "oy", "10.0.0.1");
        OuterClientService.ClusterInfo cluster2 = mockClusterInfo("cluster2", "jq", "oy", "10.0.0.2");
        List<OuterClientService.ClusterInfo> clusters = Arrays.asList(cluster1, cluster2);
        webServer.enqueue(new MockResponse().setBody(Codec.DEFAULT.encode(clusters)).setHeader("Content-Type", "application/json"));

        List<OuterClientService.ClusterInfo> resp = credisService.getActiveDcClusters("jq");
        Assert.assertEquals(clusters, resp);

        Assert.assertEquals(1, webServer.getRequestCount());
        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals("/keeperApi/queryclusters?activeDc=jq", req.getPath());
        Assert.assertEquals("GET", req.getMethod());

        MetricData metricData = metricProxy.poll();
        Assert.assertEquals("getActiveDcClusters", metricData.getTags().get("api"));
    }

    private OuterClientService.ClusterInfo mockClusterInfo(String clusterName, String activeDc, String backupDc, String redisIp) {
        OuterClientService.ClusterInfo clusterInfo = new OuterClientService.ClusterInfo();
        clusterInfo.setName(clusterName);
        clusterInfo.setIsXpipe(true);
        clusterInfo.setUsingIdc(true);
        clusterInfo.setMasterIDC(activeDc);
        clusterInfo.setRule(1);
        clusterInfo.setRuleName("读写主机房master");
        clusterInfo.setGroups(new ArrayList<>());

        OuterClientService.GroupInfo groupInfo = new OuterClientService.GroupInfo();
        groupInfo.setName(clusterName + "_shard1");
        groupInfo.setInstances(new ArrayList<>());
        clusterInfo.getGroups().add(groupInfo);

        IntStream.rangeClosed(6379, 6380).forEach(port -> {
            groupInfo.getInstances().add(mockInstanceInfo(redisIp, port, port == 6379, activeDc));
        });
        IntStream.rangeClosed(7379, 7380).forEach(port -> {
            groupInfo.getInstances().add(mockInstanceInfo(redisIp, port, false, backupDc));
        });

        return clusterInfo;
    }

    private OuterClientService.InstanceInfo mockInstanceInfo(String ip, int port, boolean isMaster, String dc) {
        OuterClientService.InstanceInfo instanceInfo = new OuterClientService.InstanceInfo();
        instanceInfo.setIPAddress(ip);
        instanceInfo.setPort(port);
        instanceInfo.setIsMaster(isMaster);
        instanceInfo.setCanRead(true);
        instanceInfo.setStatus(true);
        instanceInfo.setEnv(dc);
        return instanceInfo;
    }

}
