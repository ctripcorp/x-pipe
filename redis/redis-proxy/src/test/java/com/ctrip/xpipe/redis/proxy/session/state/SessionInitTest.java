package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class SessionInitTest extends AbstractRedisProxyServerTest {

    private SessionState sessionInit;

    private Session backend;

    @Before
    public void beforeSessionClosedTest() throws Exception {
        backend = backend();
        sessionInit = new SessionInit(backend);
    }

    @Test
    public void testDoNextAfterSuccess() {
        Assert.assertEquals(new SessionEstablished(backend), sessionInit.nextAfterSuccess());
    }

    @Test
    public void testDoNextAfterFail() {
        Assert.assertEquals(new SessionClosed(backend), sessionInit.nextAfterFail());
    }

    @Test
    public void testTryWrite() {
        sessionInit.tryWrite(new UnpooledByteBufAllocator(true).buffer().writeByte(1));
    }


    @Test
    public void testName() {
        Assert.assertEquals(sessionInit.toString(), sessionInit.name());
    }
}