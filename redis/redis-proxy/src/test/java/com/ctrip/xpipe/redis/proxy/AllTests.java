package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.proxy.session.DefaultSessionStoreTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosedTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosingTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablishedTest;
import com.ctrip.xpipe.redis.proxy.session.state.SessionInitTest;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManagerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        DefaultSessionStoreTest.class,
        DefaultTunnelManagerTest.class,
        SessionClosedTest.class,
        SessionClosingTest.class,
        SessionInitTest.class,
        SessionEstablishedTest.class,
        DefaultSessionStoreTest.class
})
public class AllTests {

}
