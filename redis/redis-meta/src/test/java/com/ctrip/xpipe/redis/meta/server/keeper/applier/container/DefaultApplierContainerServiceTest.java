package com.ctrip.xpipe.redis.meta.server.keeper.applier.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.ApplierContainerMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierTransMeta;
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
 * @author ayq
 * <p>
 * 2022/4/7 20:09
 */
public class DefaultApplierContainerServiceTest extends AbstractMetaServerTest {

    private MockWebServer webServer;

    private DefaultApplierContainerService service;

    @Before
    public void setupDefaultApplierContainerServiceTest() throws Exception {
        this.webServer = new MockWebServer();
        this.webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        this.service = new DefaultApplierContainerService(new ApplierContainerMeta().setIp("127.0.0.1").setPort(webServer.getPort()),
                RestTemplateFactory.createCommonsHttpRestTemplate());
    }

    @After
    public void afterDefaultApplierContainerServiceTest() throws Exception {
        if (null != webServer) this.webServer.close();
    }

    @Test
    public void testAddApplier() throws Exception {

        this.webServer.enqueue(new MockResponse());

        ApplierTransMeta meta = mockApplierTransMeta();
        this.service.addApplier(meta);
        RecordedRequest request = this.webServer.takeRequest();

        Assert.assertEquals(HttpMethod.POST.toString(), request.getMethod());
        Assert.assertEquals("/appliers", request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), ApplierTransMeta.class));
    }

    @Test
    public void testAddOrStartApplier() throws Exception {

        this.webServer.enqueue(new MockResponse());

        ApplierTransMeta meta = mockApplierTransMeta();
        this.service.addOrStartApplier(meta);
        RecordedRequest request = this.webServer.takeRequest();

        Assert.assertEquals(HttpMethod.POST.toString(), request.getMethod());
        Assert.assertEquals(String.format("/appliers/clusters/%s/shards/%s", meta.getClusterDbId(), meta.getShardDbId()), request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), ApplierTransMeta.class));
    }

    @Test
    public void testRemoveApplier() throws Exception {

        this.webServer.enqueue(new MockResponse());

        ApplierTransMeta meta = mockApplierTransMeta();
        this.service.removeApplier(meta);
        RecordedRequest request = this.webServer.takeRequest();

        Assert.assertEquals(HttpMethod.DELETE.toString(), request.getMethod());
        Assert.assertEquals(String.format("/appliers/clusters/%s/shards/%s", meta.getClusterDbId(), meta.getShardDbId()), request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), ApplierTransMeta.class));
    }

    @Test
    public void testStartApplier() throws Exception {

        this.webServer.enqueue(new MockResponse());

        ApplierTransMeta meta = mockApplierTransMeta();
        this.service.startApplier(meta);
        RecordedRequest request = this.webServer.takeRequest();

        Assert.assertEquals(HttpMethod.PUT.toString(), request.getMethod());
        Assert.assertEquals(String.format("/appliers/clusters/%s/shards/%s/start", meta.getClusterDbId(), meta.getShardDbId()), request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), ApplierTransMeta.class));
    }

    @Test
    public void testStopApplier() throws Exception {

        this.webServer.enqueue(new MockResponse());

        ApplierTransMeta meta = mockApplierTransMeta();
        this.service.stopApplier(meta);
        RecordedRequest request = this.webServer.takeRequest();

        Assert.assertEquals(HttpMethod.PUT.toString(), request.getMethod());
        Assert.assertEquals(String.format("/appliers/clusters/%s/shards/%s/stop", meta.getClusterDbId(), meta.getShardDbId()), request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), ApplierTransMeta.class));
    }

    private ApplierTransMeta mockApplierTransMeta() {
        return new ApplierTransMeta(1L, 1L, new ApplierMeta());
    }
}