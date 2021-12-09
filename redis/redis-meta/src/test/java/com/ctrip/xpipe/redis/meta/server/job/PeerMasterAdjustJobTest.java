package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.RouteOptionParser;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.exception.BadRedisVersionException;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterAdjustJobTest extends AbstractMetaServerTest {

    protected String clusterId = "cluster1", shardId = "shard1";

    private String version = "1.0.4";

    protected Server redisServer;

    protected Set<String> peerofRequest = new HashSet<>();

    protected Map<Long, Endpoint> currentPeerMaster = new HashMap<Long, Endpoint >(){{
        put(1L, new DefaultEndPoint("10.0.0.1", 6379)); // peer master deleted
        put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master change
        put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
    }};

    protected Map<Long, Endpoint > expectPeerMaster = new HashMap<Long, Endpoint >(){{
        put(2L, new DefaultEndPoint("10.0.0.2", 7379)); // peer master change
        put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        put(4L, new DefaultEndPoint("10.0.0.4", 6379)); // peer master added
    }};

    PeerMasterAdjustJob peerMasterAdjustJob;

    private static final String TEMP_CRDT_INFO = "peer%d_host:%s\r\n" + "peer%d_port:%d\r\n" + "peer%d_gid:%d\r\n" + "peer%d_repl_offset:%d\r\n";

    @Before
    public void setupPeerMasterChangeJobTest() throws Exception {
        mockMaster();
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
    }

    @Test
    public void testPeerMasterChange() throws Exception {
        peerMasterAdjustJob.execute().get();
        logger.info("[testPeerMasterChange] {}", peerofRequest);
        Assert.assertEquals(3, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 no one"));
        Assert.assertTrue(peerofRequest.contains("peerof 2 10.0.0.2 7379"));
        Assert.assertTrue(peerofRequest.contains("peerof 4 10.0.0.4 6379"));
    }

    @Test
    public void testClearPeerMaster() throws Exception {
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, Collections.emptyList(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        logger.info("[testPeerMasterChange] {}", peerofRequest);
        Assert.assertEquals(3, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 no one"));
        Assert.assertTrue(peerofRequest.contains("peerof 2 no one"));
        Assert.assertTrue(peerofRequest.contains("peerof 3 no one"));
    }

    @Test
    public void testAddPeerMaster() throws Exception {
        currentPeerMaster = Collections.emptyMap();
        peerMasterAdjustJob.execute().get();
        logger.info("[testPeerMasterChange] {}", peerofRequest);
        Assert.assertEquals(3, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 2 10.0.0.2 7379"));
        Assert.assertTrue(peerofRequest.contains("peerof 3 10.0.0.3 6379"));
        Assert.assertTrue(peerofRequest.contains("peerof 4 10.0.0.4 6379"));
    }

    @Test
    public void testNoDoDeleted() throws Exception {
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        logger.info("[testPeerMasterChange] {}", peerofRequest);
        Assert.assertEquals(2, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 2 10.0.0.2 7379"));
        Assert.assertTrue(peerofRequest.contains("peerof 4 10.0.0.4 6379"));
    }

    @Test
    public void testNotCRDTRedis() {
        version = null;
        try {
            peerMasterAdjustJob.execute().get();
        } catch (Exception e) {
            Assert.assertTrue(ExceptionUtils.getRootCause(e) instanceof BadRedisVersionException);
            Assert.assertEquals(0, peerofRequest.size());
            return;
        }

        Assert.fail();
    }

    @Test
    public void testNoChange() throws Exception {
        currentPeerMaster = new HashMap<>(expectPeerMaster);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(0, peerofRequest.size());
    }

    @Test
    public void testForTestVersion() throws Exception {
        version = "1.0.5-beta";
        testPeerMasterChange();
    }

    @Test
    public void testHangWhenConnectCloseOnSendRequest() throws Exception {
        scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("PeerMasterAdjustJobSchedule"));
        Bootstrap b = initBootstrap();
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                new FixedObjectPool<>(new ConnectionClosedNettyClient(b.connect("127.0.0.1", redisServer.getPort()))),
                100, 3, scheduled,  executors);

        try {
            peerMasterAdjustJob.execute().get();
        } catch (Exception e) {
            logger.info("[testConnectCloseHang] peerMasterAdjustJob fail", e);
        }
        Assert.assertEquals(0, peerofRequest.size());
    }

    @After
    public void afterPeerMasterChangeJobTest() throws Exception {
        if (null != redisServer) redisServer.stop();
    }

    /**
     *
     * @throws Exception
     *     no proxy -> proxy
     *     no proxy -> 2 proxy
     *     proxy -> 2 proxy
     *     proxy - > no proxy
     *     2 proxy -> proxy
     *     2 proxy -> no proxy
     */
    Endpoint createProxyEndpoint(String host, int port, String proxy) {
        return new ProxyEnabledEndpoint(host, port, new DefaultProxyConnectProtocolParser().read(proxy));
    }

    @Test
    public void testNoProxyToProxy() throws Exception {

        currentPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, new DefaultEndPoint("10.0.0.1", 6379)); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        expectPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, createProxyEndpoint("10.0.0.1", 6379, "PROXY ROUTE PROXYTCP://127.0.0.1:1")); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(1, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 10.0.0.1 6379 proxy-type XPIPE-PROXY proxy-servers PROXYTCP://127.0.0.1:1"));
    }

    @Test
    public void testNoProxyTo2Proxy() throws Exception {

        currentPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, new DefaultEndPoint("10.0.0.1", 6379)); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        expectPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, createProxyEndpoint("10.0.0.1", 6379, "PROXY ROUTE PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:2")); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(1, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 10.0.0.1 6379 proxy-type XPIPE-PROXY proxy-servers PROXYTCP://127.0.0.1:1 proxy-params \"PROXYTLS://127.0.0.1:2\""));
    }

    @Test
    public void test1ProxyTo2Proxy() throws Exception {

        currentPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, createProxyEndpoint("10.0.0.1", 6379, "PROXY ROUTE PROXYTCP://127.0.0.1:1")); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        expectPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, createProxyEndpoint("10.0.0.1", 6379, "PROXY ROUTE PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:2")); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(1, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 10.0.0.1 6379 proxy-type XPIPE-PROXY proxy-servers PROXYTCP://127.0.0.1:1 proxy-params \"PROXYTLS://127.0.0.1:2\""));
    }

    @Test
    public void test1ProxyToNoProxy() throws Exception {

        currentPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, createProxyEndpoint("10.0.0.1", 6379, "PROXY ROUTE PROXYTCP://127.0.0.1:1")); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        expectPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, new DefaultEndPoint("10.0.0.1", 6379)); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(1, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 10.0.0.1 6379"));
    }

    @Test
    public void test2ProxyTo1Proxy() throws Exception {

        currentPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, createProxyEndpoint("10.0.0.1", 6379, "PROXY ROUTE PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:2")); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        expectPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, createProxyEndpoint("10.0.0.1", 6379, "PROXY ROUTE PROXYTCP://127.0.0.1:1")); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(1, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 10.0.0.1 6379 proxy-type XPIPE-PROXY proxy-servers PROXYTCP://127.0.0.1:1"));
    }

    @Test
    public void test2ProxyToNoProxy() throws Exception {

        currentPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, createProxyEndpoint("10.0.0.1", 6379, "PROXY ROUTE PROXYTCP://127.0.0.1:1 PROXYTLS://127.0.0.1:2")); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        expectPeerMaster = new HashMap<Long, Endpoint >(){{
            put(1L, new DefaultEndPoint("10.0.0.1", 6379)); // peer master changed
            put(2L, new DefaultEndPoint("10.0.0.2", 6379)); // peer master unchange
            put(3L, new DefaultEndPoint("10.0.0.3", 6379)); // peer master unchange
        }};
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        Assert.assertEquals(1, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 10.0.0.1 6379"));
    }



    private List<Pair<Long, Endpoint>> mockUpstreamPeerMaster() {
        List<Pair<Long, Endpoint>> upstreamPeerMasters = new ArrayList<>();
        expectPeerMaster.forEach((gid, peerMaster) -> {
            upstreamPeerMasters.add(new Pair(gid, peerMaster));
        });

        return upstreamPeerMasters;
    }

    private String mockCRDTInfoResp() {
        StringBuilder sb = new StringBuilder();
        AtomicInteger index = new AtomicInteger(0);
        currentPeerMaster.forEach((gid, peerMaster) -> {
            sb.append(String.format(TEMP_CRDT_INFO, index.get(), peerMaster.getHost(), index.get(), peerMaster.getPort(), index.get(), gid, index.get(), randomInt()));
            ProxyConnectProtocol protocol = peerMaster.getProxyProtocol();
            if(protocol != null) {
                RouteOptionParser parser = new RouteOptionParser().read("ROUTE " + protocol.getRouteInfo());
                if( parser != null) {
                    String servers = parser.getNextEndpoints().stream().map(endpoint -> {
                        return endpoint.toString();
                    }).collect(Collectors.joining(","));
                    sb.append(String.format("peer%d_proxy_type:XPIPE-PROXY\r\npeer%d_proxy_servers:%s\r\n", index.get(), index.get(), servers));
                    String params = new RouteOptionParser().read(parser.output()).getContent();
                    if(params != null) {
                        sb.append(String.format("peer%d_proxy_params:%s\r\n", index.get(), params));
                    }
                }
            }
            index.incrementAndGet();
        });
        String content = sb.toString();
        return String.format("$%d\r\n%s\r\n", content.length(), content);
    }

    private String mockInfoServerResp() {
        String content = "";
        if (null != version) {
            content = "xredis_crdt_version:" + version;
        }
        return String.format("$%d\r\n%s\r\n", content.length(), content);
    }

    private void mockMaster() throws Exception {
        redisServer = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.startsWith("crdt.info")) {
                    return mockCRDTInfoResp();
                } else if (s.startsWith("peerof")) {
                    peerofRequest.add(s.trim());
                } else if (s.startsWith("info server")) {
                    return mockInfoServerResp();
                }

                return "+OK\r\n";
            }
        });
    }

    private Bootstrap initBootstrap() {
        Bootstrap b = new Bootstrap();
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(2, XpipeThreadFactory.create("NettyKeyedPoolClientFactory"));
        b.group(eventLoopGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(512))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler());
                        p.addLast(new NettySimpleMessageHandler());
                        p.addLast(new NettyClientHandler());
                    }
                });
        return b;
    }

    private static class ConnectionClosedNettyClient extends DefaultNettyClient {

        private ChannelFuture future;

        public ConnectionClosedNettyClient(ChannelFuture future) {
            super(future.channel());
            this.future = future;
        }

        @Override
        public void sendRequest(ByteBuf byteBuf) {
            future.channel().close();
            super.sendRequest(byteBuf);
        }

    }

}
