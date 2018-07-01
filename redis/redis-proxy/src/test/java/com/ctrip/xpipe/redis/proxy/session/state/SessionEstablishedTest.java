package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class SessionEstablishedTest extends AbstractRedisProxyServerTest {

    private SessionState sessionEstablished;

    private Session frontend;

    @Before
    public void beforeSessionClosedTest() throws Exception {
        frontend = frontend();
        sessionEstablished = new SessionEstablished(frontend);
    }

    @Test
    public void testDoNextAfterSuccess() {
        Assert.assertEquals(new SessionEstablished(frontend), sessionEstablished.nextAfterSuccess());
    }

    @Test
    public void testDoNextAfterFail() {
        Assert.assertEquals(new SessionClosing(frontend), sessionEstablished.nextAfterFail());
    }

    @Test
    @DirtiesContext
    public void testTryWrite() {
        sessionEstablished.tryWrite(new UnpooledByteBufAllocator(true).buffer());
    }

    @Test
    public void testName() {
        Assert.assertEquals(sessionEstablished.toString(), sessionEstablished.name());
    }
}