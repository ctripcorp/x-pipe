package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import io.netty.channel.ConnectTimeoutException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2021/9/24
 */
public class DefaultReactorMetaServerConsoleServiceTest extends AbstractTest {

    private DefaultReactorMetaServerConsoleService service;

    private MockWebServer webServer;

    private String cluster = "cluster1", shard = "shard1", currentDc = "jq", targetDc = "oy";

    @Before
    public void setupDefaultReactorMetaServerConsoleServiceTest() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        service = new DefaultReactorMetaServerConsoleService("http://127.0.0.1:" + webServer.getPort(), loopResources, connectionProvider);
    }

    @Test
    public void testPreMigrationCheck() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"errorType\": \"SUCCESS\",\n" +
                "    \"errorMessage\": \"test\"\n" +
                "}")
                .setHeader("Content-Type", "application/json"));
        MetaServerConsoleService.PrimaryDcCheckMessage resp = service.changePrimaryDcCheck(cluster, shard, targetDc).get(1, TimeUnit.SECONDS);

        Assert.assertEquals(MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT.SUCCESS, resp.getErrorType());

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(String.format("/api/meta/changeprimarydc/check/%s/%s/%s", cluster, shard, targetDc), req.getPath());
        Assert.assertEquals("GET", req.getMethod());
    }

    @Test
    public void testMakeMasterReadonly() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"masterAddr\": \"10.0.0.1:6379\",\n" +
                "    \"masterInfo\": {\"replId\": \"123\", \"masterReplOffset\": 0},\n" +
                "    \"message\": \"test\"\n" +
                "}")
                .setHeader("Content-Type", "application/json"));
        MetaServerConsoleService.PreviousPrimaryDcMessage resp = service.makeMasterReadOnly(cluster, shard, true).get(1, TimeUnit.SECONDS);

        Assert.assertEquals(new HostPort("10.0.0.1", 6379), resp.getMasterAddr());

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(String.format("/api/meta/masterreadonly/%s/%s/true", cluster, shard), req.getPath());
        Assert.assertEquals("PUT", req.getMethod());
    }

    @Test
    public void testChangePrimaryDc() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"newMasterIp\": \"10.0.0.1\",\n" +
                "    \"newMasterPort\": 6379,\n" +
                "    \"errorType\": \"SUCCESS\",\n" +
                "    \"errorMessage\": \"test\"\n" +
                "}")
                .setHeader("Content-Type", "application/json"));
        MetaServerConsoleService.PrimaryDcChangeMessage resp = service.doChangePrimaryDc(cluster, shard, targetDc,
                new MetaServerConsoleService.PrimaryDcChangeRequest(new MasterInfo("123", 0L))).get(1, TimeUnit.SECONDS);

        Assert.assertEquals(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, resp.getErrorType());

        RecordedRequest req = webServer.takeRequest();
        Assert.assertEquals(String.format("/api/meta/changeprimarydc/%s/%s/%s", cluster, shard, targetDc), req.getPath());
        Assert.assertEquals("123",
                Codec.DEFAULT.decode(req.getBody().readByteArray(), MetaServerConsoleService.PrimaryDcChangeRequest.class).getMasterInfo().getReplId());
        Assert.assertEquals("PUT", req.getMethod());
    }

    @Test
    public void testChangeOtherDcPrimaryDc() throws Exception {
        webServer.enqueue(new MockResponse().setBody("{\n" +
                "    \"newMasterIp\": \"10.0.0.1\",\n" +
                "    \"newMasterPort\": 6379,\n" +
                "    \"errorType\": \"SUCCESS\",\n" +
                "    \"errorMessage\": \"test\"\n" +
                "}")
                .setHeader("Content-Type", "application/json"));
        MetaServerConsoleService.PrimaryDcChangeMessage resp = service.doChangePrimaryDc(cluster, shard, targetDc, null)
                .get(1, TimeUnit.SECONDS);
        Assert.assertEquals(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, resp.getErrorType());
    }

    @Test
    public void testConnectTimeout() throws Exception {
        service = new DefaultReactorMetaServerConsoleService("http://10.0.0.1:80", loopResources, connectionProvider);
        long start = System.currentTimeMillis();

        try {
            logger.info("[testConnectTimeout] start at {}", start);
            service.makeMasterReadOnly(cluster, shard, true).get();
            Assert.fail();
        } catch (ExecutionException e) {
            long end = System.currentTimeMillis();
            logger.info("[testConnectTimeout] end at {}", end);

            Assert.assertTrue(end - start >= 2000); // 2 * connect timeout milli
            Assert.assertTrue(e.getCause() instanceof ConnectTimeoutException);
        }
    }

}
