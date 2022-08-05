package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.proxy.handler.*;
import com.ctrip.xpipe.redis.proxy.handler.response.ProxyMonitorHandlerTest;
import com.ctrip.xpipe.redis.proxy.integrate.TestCloseOnBothSide;
import com.ctrip.xpipe.redis.proxy.integrate.TestMassTCPPacketWithOneProxyServer;
import com.ctrip.xpipe.redis.proxy.integrate.TestTLSWithTwoProxy;
import com.ctrip.xpipe.redis.proxy.monitor.DefaultTunnelMonitorManagerTest;
import com.ctrip.xpipe.redis.proxy.monitor.session.DefaultOutboundBufferMonitorTest;
import com.ctrip.xpipe.redis.proxy.monitor.session.DefaultSessionMonitorTest;
import com.ctrip.xpipe.redis.proxy.monitor.session.DefaultSessionStatsTest;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultPingStatsManagerTest;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultPingStatsTest;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultSocketStatsManagerTest;
import com.ctrip.xpipe.redis.proxy.monitor.tunnel.DefaultTunnelMonitorTest;
import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSessionTest;
import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSessionTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosedTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosingTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablishedTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInitTest;
import com.ctrip.xpipe.redis.proxy.tunnel.BothSessionTryWriteTest;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        SessionClosedTest.class,
        SessionClosingTest.class,
        SessionInitTest.class,
        SessionEstablishedTest.class,

        ZstdEncoderTest.class,
        ZstdDecoderTest.class,

        AbstractSessionNettyHandlerTest.class,
        FrontendSessionNettyHandlerTest.class,
        BackendSessionHandlerTest.class,
        ProxyConnectProtocolDecoderTest.class,

        DefaultTunnelTest.class,
        DefaultBackendSessionTest.class,
        DefaultFrontendSessionTest.class,

        DefaultSocketStatsManagerTest.class,

        TestCloseOnBothSide.class,
        InternalNetworkHandlerTest.class,
        BothSessionTryWriteTest.class,
        DefaultSessionStatsTest.class,
        ProxyPingHandlerTest.class,
        DefaultSessionMonitorTest.class,
        DefaultOutboundBufferMonitorTest.class,
        DefaultTunnelMonitorTest.class,
        DefaultTunnelMonitorManagerTest.class,
        DefaultPingStatsManagerTest.class,
        ProxyMonitorHandlerTest.class,
        DefaultPingStatsTest.class,
        TestTLSWithTwoProxy.class,
        TestMassTCPPacketWithOneProxyServer.class,

})
public class AllTests {

}
