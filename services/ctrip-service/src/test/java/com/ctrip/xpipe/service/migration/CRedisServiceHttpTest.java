package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.service.AbstractServiceTest;
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

    @Before
    public void setupCRedisServiceHttpTest() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        when(config.get(matches(CRedisConfig.KEY_CREDIS_SERVEICE_ADDRESS), anyString())).thenReturn("127.0.0.1:" + webServer.getPort());
        CRedisConfig.INSTANCE.setConfig(config);
    }

    @After
    public void afterCRedisServiceHttpTest() throws Exception {
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
    }

}
