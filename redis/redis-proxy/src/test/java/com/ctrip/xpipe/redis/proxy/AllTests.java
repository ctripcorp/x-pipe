package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.proxy.config.DefaultProxyConfigTest;
import com.ctrip.xpipe.redis.proxy.handler.*;
import com.ctrip.xpipe.redis.proxy.integrate.TestCloseOnBothSide;
import com.ctrip.xpipe.redis.proxy.integrate.TestMassTCPPacketWithOneProxyServer;
import com.ctrip.xpipe.redis.proxy.integrate.TestTLSWithTwoProxy;
import com.ctrip.xpipe.redis.proxy.session.DefaultBackendSessionTest;
import com.ctrip.xpipe.redis.proxy.session.DefaultFrontendSessionTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosedTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosingTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablishedTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInitTest;
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
        DefaultProxyConfigTest.class,

        AbstractSessionNettyHandlerTest.class,
        FrontendSessionNettyHandlerTest.class,
        BackendSessionHandlerTest.class,
        ProxyProtocolDecoderTest.class,

        DefaultTunnelTest.class,
        DefaultBackendSessionTest.class,
        DefaultFrontendSessionTest.class,

        TestCloseOnBothSide.class,
        InternalNetworkHandlerTest.class
//        TestMassTCPPacketWithOneProxyServer.class,
//        TestTLSWithTwoProxy.class

})
public class AllTests {

}
