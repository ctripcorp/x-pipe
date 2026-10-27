package com.ctrip.xpipe.redis.meta.server.keeper.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.HostNameVerification;
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
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        Assert.assertEquals(String.format("/keepers/clusters/%s/shards/%s", meta.getClusterDbId(), meta.getShardDbId()), request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), KeeperTransMeta.class));
    }

    @Test
    public void testRemoveWithReplId() throws Exception {
        this.webServer.enqueue(new MockResponse());
        KeeperTransMeta meta = new KeeperTransMeta(1L, new KeeperMeta());
        this.service.removeKeeper(meta);
        RecordedRequest request = this.webServer.takeRequest();
        Assert.assertEquals(HttpMethod.DELETE.toString(), request.getMethod());
        Assert.assertEquals("/keepers/clusters/1/shards/1", request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), KeeperTransMeta.class));
    }

    @Test
    public void testReleaseKeeper() throws Exception {
        this.webServer.enqueue(new MockResponse());

        KeeperTransMeta meta = mockKeeperMeta();
        this.service.releaseKeeper(meta);
        RecordedRequest request = this.webServer.takeRequest();

        Assert.assertEquals(HttpMethod.PUT.toString(), request.getMethod());
        Assert.assertEquals(String.format("/keepers/clusters/%s/shards/%s/release", meta.getClusterDbId(), meta.getShardDbId()), request.getPath());
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), KeeperTransMeta.class));
    }

    @Test
    public void testResolveByHostname() throws Exception {
        // 准备 mock addressService
        CurrentDcKeeperContainerAddressService addressService = mock(CurrentDcKeeperContainerAddressService.class);
        when(addressService.getIpByHostname("pod-a-0")).thenReturn("127.0.0.1");

        // 容器形态: IP 为空,有 hostname
        KeeperContainerMeta containerMeta = new KeeperContainerMeta()
                .setIp("")  // 容器登记时无 IP
                .setHostname("pod-a-0")
                .setPort(webServer.getPort());
        
        DefaultKeeperContainerService podService = new DefaultKeeperContainerService(
                containerMeta, RestTemplateFactory.createCommonsHttpRestTemplate(), addressService);

        this.webServer.enqueue(new MockResponse());
        KeeperTransMeta meta = mockKeeperMeta();
        podService.addKeeper(meta);
        
        RecordedRequest request = this.webServer.takeRequest();
        Assert.assertEquals(HttpMethod.POST.toString(), request.getMethod());
        Assert.assertEquals("/keepers", request.getPath());
        
        // 验证请求头带有 hostname
        String hostnameHeader = request.getHeader(HostNameVerification.HEADER_TARGET_HOST_NAME);
        Assert.assertEquals("pod-a-0", hostnameHeader);
        
        // 验证请求体
        Assert.assertEquals(meta, Codec.DEFAULT.decode(request.getBody().readByteArray(), KeeperTransMeta.class));
    }

    @Test
    public void testResolveByHostnameWithZeroIp() throws Exception {
        // 准备 mock addressService
        CurrentDcKeeperContainerAddressService addressService = mock(CurrentDcKeeperContainerAddressService.class);
        when(addressService.getIpByHostname("pod-b-0")).thenReturn("127.0.0.1");

        // 容器形态: IP 为 0.0.0.0,有 hostname
        KeeperContainerMeta containerMeta = new KeeperContainerMeta()
                .setIp("0.0.0.0")  // 容器登记时 IP 为默认值
                .setHostname("pod-b-0")
                .setPort(webServer.getPort());
        
        DefaultKeeperContainerService podService = new DefaultKeeperContainerService(
                containerMeta, RestTemplateFactory.createCommonsHttpRestTemplate(), addressService);

        this.webServer.enqueue(new MockResponse());
        KeeperTransMeta meta = mockKeeperMeta();
        podService.removeKeeper(meta);
        
        RecordedRequest request = this.webServer.takeRequest();
        Assert.assertEquals(HttpMethod.DELETE.toString(), request.getMethod());
        Assert.assertEquals(String.format("/keepers/clusters/%s/shards/%s", meta.getClusterDbId(), meta.getShardDbId()), request.getPath());
        
        // 验证请求头带有 hostname
        String hostnameHeader = request.getHeader(HostNameVerification.HEADER_TARGET_HOST_NAME);
        Assert.assertEquals("pod-b-0", hostnameHeader);
    }

    @Test
    public void testPhysicalMachineNoHostnameHeader() throws Exception {
        // 物理机形态: IP 有值, hostname 为空,不应该带 hostname header
        this.webServer.enqueue(new MockResponse());
        KeeperTransMeta meta = mockKeeperMeta();
        this.service.removeKeeper(meta);
        
        RecordedRequest request = this.webServer.takeRequest();
        
        // 验证请求头不包含 hostname
        String hostnameHeader = request.getHeader(HostNameVerification.HEADER_TARGET_HOST_NAME);
        Assert.assertNull(hostnameHeader);
    }

    private KeeperTransMeta mockKeeperMeta() {
        return new KeeperTransMeta(1L, 1L, new KeeperMeta());
    }

    @Test
    public void testListStoreDirNames() throws Exception {
        this.webServer.enqueue(new MockResponse().setBody(Codec.DEFAULT.encode(Arrays.asList("repl_1", "repl_2")))
                .addHeader("Content-Type", "application/json"));

        List<String> dirs = this.service.listStoreDirNames();

        RecordedRequest request = this.webServer.takeRequest();
        Assert.assertEquals(HttpMethod.GET.toString(), request.getMethod());
        Assert.assertEquals("/keepers/gc/dirs", request.getPath());
        Assert.assertEquals(Arrays.asList("repl_1", "repl_2"), dirs);
    }

    @Test
    public void testGcOrphanStore() throws Exception {
        GcOrphanStoreResult expected = GcOrphanStoreResult.deleted(42L);
        this.webServer.enqueue(new MockResponse().setBody(Codec.DEFAULT.encode(expected))
                .addHeader("Content-Type", "application/json"));

        GcOrphanStoreResult result = this.service.gcOrphanStore(42L);

        RecordedRequest request = this.webServer.takeRequest();
        Assert.assertEquals(HttpMethod.PUT.toString(), request.getMethod());
        Assert.assertEquals("/keepers/gc/42", request.getPath());
        Assert.assertEquals(GcOrphanStoreResult.Status.DELETED, result.status);
        Assert.assertEquals(expected.replId, result.replId);
    }

    @Test
    public void testGcOrphanStores() throws Exception {
        List<GcOrphanStoreResult> expected = Arrays.asList(GcOrphanStoreResult.deleted(1L), GcOrphanStoreResult.notFound(2L));
        this.webServer.enqueue(new MockResponse().setBody(Codec.DEFAULT.encode(expected))
                .addHeader("Content-Type", "application/json"));

        List<GcOrphanStoreResult> result = this.service.gcOrphanStores(Arrays.asList(1L, 2L));

        RecordedRequest request = this.webServer.takeRequest();
        Assert.assertEquals(HttpMethod.PUT.toString(), request.getMethod());
        Assert.assertEquals("/keepers/gc", request.getPath());
        Assert.assertEquals("[1,2]", request.getBody().readUtf8());
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(GcOrphanStoreResult.Status.DELETED, result.get(0).status);
        Assert.assertEquals(GcOrphanStoreResult.Status.NOT_FOUND, result.get(1).status);
    }

}
