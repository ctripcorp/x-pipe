package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.metaserver.DcMetaReloadResult;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaReloader;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DcMetaReloadControllerTest {

    private static final long LOAD_TIME = 1700000000000L;

    @InjectMocks
    private DcMetaReloadController controller;

    @Mock
    private DcMetaReloader dcMetaReloader;

    @Mock
    private ClusterServers clusterServers;

    @Mock
    private CurrentClusterServer currentClusterServer;

    private final Map<Integer, ClusterServerInfo> servers = new HashMap<>();

    private HttpServer peer;

    @Before
    public void setUp() throws Exception {
        when(currentClusterServer.getServerId()).thenReturn(1);
        when(currentClusterServer.getClusterInfo()).thenReturn(new ClusterServerInfo("127.0.0.1", 9747));
        servers.put(1, new ClusterServerInfo("127.0.0.1", 9747));
        when(clusterServers.allClusterServerInfos()).thenReturn(servers);
        when(dcMetaReloader.pull()).thenReturn(new DcMetaReloader.Pulled(dcMeta(), LOAD_TIME));
        when(dcMetaReloader.apply(any(), anyLong())).thenReturn(DcMetaReloader.ReloadStatus.APPLIED);
    }

    @After
    public void tearDown() {
        controller.destroy();
        if (peer != null) {
            peer.stop(0);
        }
    }

    @Test
    public void testSingleInstancePullsOnceAndApplies() throws Exception {
        List<DcMetaReloadResult> results = controller.reload();

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(DcMetaReloadResult.Status.APPLIED, results.get(0).getStatus());
        verify(dcMetaReloader, times(1)).pull();
        verify(dcMetaReloader).apply(any(), eq(LOAD_TIME));
    }

    @Test
    public void testPeerGetsSameDcMetaWithoutConsoleCall() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        AtomicReference<String> query = new AtomicReference<>();
        startPeer(body, query, "{\"serverId\":0,\"status\":\"APPLIED\"}");

        List<DcMetaReloadResult> results = controller.reload();

        Assert.assertEquals(results.toString(), 2, results.size());
        Assert.assertEquals(DcMetaReloadResult.Status.APPLIED, results.get(1).getStatus());
        Assert.assertEquals(2, results.get(1).getServerId());
        // one console pull for the whole dc; the peer only received the encoded DcMeta
        verify(dcMetaReloader, times(1)).pull();
        Assert.assertEquals("loadTime=" + LOAD_TIME, query.get());
        DcMeta forwarded = JsonCodec.INSTANCE.decode(body.get(), DcMeta.class);
        Assert.assertNotNull(forwarded.findCluster("cluster1"));
    }

    @Test
    public void testPeerStatusPassedThrough() throws Exception {
        startPeer(new AtomicReference<>(), new AtomicReference<>(), "{\"status\":\"SKIPPED_RECENT_MODIFY\"}");

        List<DcMetaReloadResult> results = controller.reload();

        Assert.assertEquals(DcMetaReloadResult.Status.SKIPPED_RECENT_MODIFY, results.get(1).getStatus());
    }

    @Test
    public void testUnreachablePeerReportedFailed() throws Exception {
        // nothing listens on port 1: connection refused
        servers.put(2, new ClusterServerInfo("127.0.0.1", 1));

        List<DcMetaReloadResult> results = controller.reload();

        Assert.assertEquals(DcMetaReloadResult.Status.APPLIED, results.get(0).getStatus());
        DcMetaReloadResult peerResult = results.get(1);
        Assert.assertEquals(2, peerResult.getServerId());
        Assert.assertEquals("127.0.0.1:1", peerResult.getAddress());
        Assert.assertEquals(DcMetaReloadResult.Status.FAILED, peerResult.getStatus());
    }

    @Test
    public void testPullRetriedOnce() throws Exception {
        when(dcMetaReloader.pull()).thenThrow(new IllegalStateException("getDcMeta Overflow"))
                .thenReturn(new DcMetaReloader.Pulled(dcMeta(), LOAD_TIME));

        Assert.assertEquals(DcMetaReloadResult.Status.APPLIED, controller.reload().get(0).getStatus());
        verify(dcMetaReloader, times(2)).pull();
    }

    @Test
    public void testPullFailedReportsEveryInstance() throws Exception {
        when(dcMetaReloader.pull()).thenThrow(new IllegalStateException("getDcMeta Overflow"));
        servers.put(2, new ClusterServerInfo("127.0.0.2", 8080));

        List<DcMetaReloadResult> results = controller.reload();

        Assert.assertEquals(2, results.size());
        for (DcMetaReloadResult result : results) {
            Assert.assertEquals(DcMetaReloadResult.Status.FAILED, result.getStatus());
            Assert.assertTrue(result.getMessage(), result.getMessage().contains("Overflow"));
        }
        verify(dcMetaReloader, times(1 + DcMetaReloadController.PULL_RETRY)).pull();
        verify(dcMetaReloader, never()).apply(any(), anyLong());
    }

    @Test
    public void testLocalStatusPassedThrough() throws Exception {
        when(dcMetaReloader.apply(any(), anyLong())).thenReturn(DcMetaReloader.ReloadStatus.REJECTED_TOO_MANY);

        Assert.assertEquals(DcMetaReloadResult.Status.REJECTED_TOO_MANY, controller.reload().get(0).getStatus());
    }

    @Test
    public void testApplyEndpointDecodesAndApplies() throws Exception {
        ArgumentCaptor<DcMeta> captor = ArgumentCaptor.forClass(DcMeta.class);

        DcMetaReloadResult result = controller.apply(LOAD_TIME, JsonCodec.INSTANCE.encode(dcMeta()));

        Assert.assertEquals(DcMetaReloadResult.Status.APPLIED, result.getStatus());
        verify(dcMetaReloader).apply(captor.capture(), eq(LOAD_TIME));
        Assert.assertNotNull(captor.getValue().findCluster("cluster1"));
        verify(dcMetaReloader, never()).pull();
    }

    @Test
    public void testApplyEndpointBadBody() throws Exception {
        DcMetaReloadResult result = controller.apply(LOAD_TIME, "not json");

        Assert.assertEquals(DcMetaReloadResult.Status.FAILED, result.getStatus());
        verify(dcMetaReloader, never()).apply(any(), anyLong());
    }

    private void startPeer(AtomicReference<String> body, AtomicReference<String> query, String response) throws Exception {
        peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peer.createContext(DcMetaReloadController.PATH_APPLY, ex -> {
            query.set(ex.getRequestURI().getRawQuery());
            body.set(readAll(ex.getRequestBody()));
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(bytes);
            }
        });
        peer.start();
        servers.put(2, new ClusterServerInfo("127.0.0.1", peer.getAddress().getPort()));
    }

    private static String readAll(InputStream in) throws java.io.IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private static DcMeta dcMeta() {
        DcMeta dcMeta = new DcMeta("jq");
        dcMeta.addCluster(new ClusterMeta("cluster1").setDbId(1L));
        return dcMeta;
    }
}
