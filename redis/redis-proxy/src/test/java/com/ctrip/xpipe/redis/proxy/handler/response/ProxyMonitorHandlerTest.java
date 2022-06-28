package com.ctrip.xpipe.redis.proxy.handler.response;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.monitor.*;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.model.TunnelIdentity;
import com.ctrip.xpipe.redis.proxy.monitor.SessionMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.session.SessionStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStatsManager;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.TunnelStats;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelEstablished;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ProxyMonitorHandlerTest extends AbstractProxyIntegrationTest {

    private TunnelManager tunnelManager;

    private PingStatsManager pingStatsManager;

    private ProxyMonitorHandler handler;

    private TunnelMonitor tunnelMonitor;

    private String template1 = "ESTAB      0      0             10.26.188.107:47862         10.15.206.22:443";

    private String template2 = "skmem:(r0,rb235128,t0,tb87380,f0,w0,o0,bl0) cubic wscale:7,10 rto:458 rtt:176.498/45.981 ato:40 mss:1448 cwnd:10 send 656.3Kbps rcv_rtt:175 rcv_space:26883";

    private String template3 = "SYN-SENT   0      1             10.26.188.107:49766        10.26.190.167:9092";

    private String template4 = "skmem:(r0,rb87380,t0,tb87380,f4294966016,w1280,o0,bl0) cubic rto:4000 mss:524 cwnd:1 ssthresh:7 unacked:1 retrans:1/2 lost:1";

    @Before
    public void beforeProxyMonitorHandlerTest() {

        Tunnel tunnel = mock(Tunnel.class);
        tunnelMonitor = mock(TunnelMonitor.class);
        SessionMonitor frontend = mock(SessionMonitor.class);
        SessionMonitor backend = mock(SessionMonitor.class);

        SocketStats socketStats1 = mock(SocketStats.class);
        when(socketStats1.getSocketStatsResult()).thenReturn(new SocketStatsResult(Lists.newArrayList(template1, template2)));
        when(frontend.getSocketStats()).thenReturn(socketStats1);
        SocketStats socketStats2 = mock(SocketStats.class);
        when(socketStats2.getSocketStatsResult()).thenReturn(new SocketStatsResult(Lists.newArrayList(template3, template4)));
        when(backend.getSocketStats()).thenReturn(socketStats2);

        SessionStats frontendSessionStats = mock(SessionStats.class);
        SessionStats backendSessionStats = mock(SessionStats.class);
        when(frontend.getSessionStats()).thenReturn(frontendSessionStats);
        when(backend.getSessionStats()).thenReturn(backendSessionStats);

        when(tunnelMonitor.getFrontendSessionMonitor()).thenReturn(frontend);
        when(tunnelMonitor.getBackendSessionMonitor()).thenReturn(backend);

        when(tunnel.getTunnelMonitor()).thenReturn(tunnelMonitor);
        Channel frontChannel = mock(Channel.class);
        when(frontChannel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 6379));

        Channel backChannel = mock(Channel.class);
        when(backChannel.remoteAddress()).thenReturn(new InetSocketAddress("10.26.189.90", 6379));
        when(backChannel.localAddress()).thenReturn(new InetSocketAddress("10.2.3.23", 8080));
        when(tunnel.identity()).thenReturn(
                new TunnelIdentity(frontChannel, "10.3.2.23:6379", "10.26.189.90:6379").setBackend(backChannel));

        tunnelManager = mock(TunnelManager.class);
        when(tunnelManager.tunnels()).thenReturn(Lists.newArrayList(tunnel));

        pingStatsManager = mock(PingStatsManager.class);
        handler = new ProxyMonitorHandler(tunnelManager, pingStatsManager, new TestProxyConfig());
    }

    @Test
    public void testSocketStatsResponser() throws TimeoutException {

        AtomicReference<ByteBuf> result = new AtomicReference<>();
        Channel channel = getWriteBackChannel(result);
        handler.handle(channel, new String[]{"SocketStats"});
        waitConditionUntilTimeOut(()->result.get() != null, 1000);
//        logger.info("{}", ByteBufUtils.readToString(result.get()));
        ArrayParser parser = (ArrayParser) new ArrayParser().read(result.get());
        Object[] objects = parser.getPayload();
        for(Object object : objects) {
            TunnelSocketStatsResult tunnelSocketStatsResult = TunnelSocketStatsResult.parse(object);
            String tunnelId = tunnelSocketStatsResult.getTunnelId();
            logger.info("{}", tunnelId);
            SocketStatsResult ret1 = tunnelSocketStatsResult.getFrontendSocketStats();
            SocketStatsResult ret2 = tunnelSocketStatsResult.getBackendSocketStats();
            logger.info("{}", DateTimeUtils.timeAsString(ret1.getTimestamp()));
            logger.info("{}", ret1.getResult());
            logger.info("{}", DateTimeUtils.timeAsString(ret2.getTimestamp()));
            logger.info("{}", ret2.getResult());
        }

    }

    @Test
    public void testWhenNPE() {
        ProxyMonitorHandler handler = new ProxyMonitorHandler(null, null, new TestProxyConfig());
        AtomicReference<ByteBuf> result = new AtomicReference<>();
        Channel channel = getWriteBackChannel(result);
        handler.handle(channel, new String[]{"PingStats"});

        RedisError error = new RedisErrorParser().read(result.get()).getPayload();
        assertTrue(error.getMessage().startsWith("-PROXY THROWABLE java.lang.NullPointerException"));
    }

    @Test
    public void testPingStatsResponser() throws TimeoutException {
        PingStats pingStats = mock(PingStats.class);
        when(pingStats.getPingStatsResult()).thenReturn(new PingStatsResult(System.currentTimeMillis() - 2,
                System.currentTimeMillis(), localHostport(randomPort()),
                localHostport(randomPort())));
        when(pingStatsManager.getAllPingStats()).thenReturn(Lists.newArrayList(pingStats));

        AtomicReference<ByteBuf> result = new AtomicReference<>();
        Channel channel = getWriteBackChannel(result);
        handler.doHandle(channel, new String[]{"PingStats"});
        waitConditionUntilTimeOut(()->result.get() != null, 1000);
//        logger.info("{}", ByteBufUtils.readToString(result.get()));
        ArrayParser parser = (ArrayParser) new ArrayParser().read(result.get());
        Object[] objects = parser.getPayload();
        for(Object object : objects) {
            PingStatsResult pingStatsResult = PingStatsResult.parse(object);
            logger.info("{}", pingStatsResult.getDirect());
            logger.info("{}", pingStatsResult.getReal());
            logger.info("{}", pingStatsResult.getStart());
            logger.info("{}", pingStatsResult.getEnd());
        }

    }


    @Test
    public void testTunnelStatsResponser() throws Exception {
        TunnelStats tunnelStats = mock(TunnelStats.class);

        HostPort frontend = localHostport(randomPort());
        HostPort backend = localHostport(randomPort());

        Tunnel tunnel = tunnelManager.tunnels().get(0);
        TunnelStatsResult tunnelStatsResult = new TunnelStatsResult(tunnel.identity().toString(),
                new TunnelEstablished(null).name(), System.currentTimeMillis(), System.currentTimeMillis() + 10, frontend, backend);
        when(tunnelStats.getTunnelStatsResult()).thenReturn(tunnelStatsResult);
        when(tunnelMonitor.getTunnelStats()).thenReturn(tunnelStats);

        AtomicReference<ByteBuf> result = new AtomicReference<>();
        Channel channel = getWriteBackChannel(result);
        handler.doHandle(channel, new String[]{"TunnelStats"});
        waitConditionUntilTimeOut(()->result.get() != null, 1000);
//        logger.info("{}", ByteBufUtils.readToString(result.get()));
        ArrayParser parser = (ArrayParser) new ArrayParser().read(result.get());
        Object[] objects = parser.getPayload();
        for(Object object : objects) {
            TunnelStatsResult other = TunnelStatsResult.parse(object);
            logger.info("{}", other.getTunnelId());
            logger.info("{}", other.getTunnelState());
            logger.info("{}", other.getProtocolRecvTime());
            logger.info("{}", other.getProtocolSndTime());
            logger.info("{}", other.getFrontend());
            logger.info("{}", other.getBackend());
        }
    }

    @Test
    public void testTrafficStatsResponser() throws Exception {

        SessionTrafficResult frontend = new SessionTrafficResult(System.currentTimeMillis(), 100, 200, 10, 20);
        SessionTrafficResult backend = new SessionTrafficResult(System.currentTimeMillis(), 1000, 2000, 100, 200);

        Tunnel tunnel = tunnelManager.tunnels().get(0);
        TunnelTrafficResult trafficResult = new TunnelTrafficResult(tunnel.identity().toString(), frontend, backend);
        when(tunnel.getTunnelMonitor().getFrontendSessionMonitor().getSessionStats().getInputBytes()).thenReturn(frontend.getInputBytes());
        when(tunnel.getTunnelMonitor().getFrontendSessionMonitor().getSessionStats().getOutputBytes()).thenReturn(frontend.getOutputBytes());
        when(tunnel.getTunnelMonitor().getFrontendSessionMonitor().getSessionStats().lastUpdateTime()).thenReturn(frontend.getTimestamp());

        when(tunnel.getTunnelMonitor().getBackendSessionMonitor().getSessionStats().getInputBytes()).thenReturn(backend.getInputBytes());
        when(tunnel.getTunnelMonitor().getBackendSessionMonitor().getSessionStats().getOutputBytes()).thenReturn(backend.getOutputBytes());
        when(tunnel.getTunnelMonitor().getBackendSessionMonitor().getSessionStats().lastUpdateTime()).thenReturn(backend.getTimestamp());


        AtomicReference<ByteBuf> result = new AtomicReference<>();
        Channel channel = getWriteBackChannel(result);
        handler.doHandle(channel, new String[]{"TrafficStats"});
        waitConditionUntilTimeOut(()->result.get() != null, 1000);
//        logger.info("{}", ByteBufUtils.readToString(result.get()));
        TunnelTrafficResult result1 = TunnelTrafficResult.parse(new ArrayParser().read(result.get()).getPayload()[0]);
        logger.info("{}", result1);
    }

    private Channel getWriteBackChannel(AtomicReference<ByteBuf> result) {
        Channel channel = mock(Channel.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                result.set(invocation.getArgument(0, ByteBuf.class));
                return null;
            }
        }).when(channel).writeAndFlush(any(ByteBuf.class));
        return  channel;
    }

    @Ignore
    @Test
    public void testArrayParser() {
        Object[][] resultSet = new Object[2][];
        resultSet[0] = new Object[2];
        resultSet[0][0] = "hello";
        resultSet[0][1] = "world";
        resultSet[1] = new Object[2];
        resultSet[1][0] = "jajaja";
        resultSet[1][1] = "jajaja";
        ArrayParser parser = new ArrayParser(resultSet);
        ByteBuf byteBuf = parser.format();

        logger.info("[result] \r\n{}", ByteBufUtils.readToString(byteBuf));

        byteBuf = parser.format();
        ArrayParser receiver = new ArrayParser();
        receiver = (ArrayParser) receiver.read(byteBuf);

        Object[] objects = receiver.getPayload();
        Assert.assertNotNull(objects);
        for(Object obj : objects) {
            logger.info("{}", obj);
        }
    }

}