package com.ctrip.xpipe.redis.meta.server.keeper.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpMethod;

import java.net.InetAddress;

/**
 * @author lishanglin
 * date 2021/12/4
 */
public class DefaultKeeperContainerServiceTest extends AbstractMetaServerTest {

    private MockWebServer webServer;

    private DefaultKeeperContainerService service;

    @Before
    public void setupDefaultKeeperContainerServiceTest() throws Exception {
        this.webServer = new MockWebServer();
        this.webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        this.service = new DefaultKeeperContainerService(new KeeperContainerMeta().setIp("127.0.0.1").setPort(webServer.getPort()),
                RestTemplateFactory.createCommonsHttpRestTemplate());
    }

    @After
    public void afterDefaultKeeperContainerServiceTest() throws Exception {
        if (null != webServer) this.webServer.close();
    }

    @Test
    public void testRemoveKeeper() throws Exception {
        this.webServer.enqueue(new MockResponse());

        KeeperTransMeta meta = mockKeeperMeta();
        this.service.removeKeeper(meta);
        RecordedRequest request = this.webServer.takeRequest();

        Assert.assertEquals(HttpMethod.DELETE.toString(), request.getMethod());
        Assert.assertEquals(String.format("/keepers/clusters/%s/shards/%s", meta.getClusterId(), meta.getShardId()), request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), KeeperTransMeta.class));
    }

    private KeeperTransMeta mockKeeperMeta() {
        return new KeeperTransMeta("cluster1", "shard1", 1L, 1L, new KeeperMeta());
    }

}
