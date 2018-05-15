package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.exception.WriteWhenSessionInitException;
import com.ctrip.xpipe.redis.proxy.session.DefaultSession;
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

    private DefaultSession backend;

    @Before
    public void beforeSessionClosedTest() throws Exception {
        backend = (DefaultSession) backend();
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

    @Test(expected = WriteWhenSessionInitException.class)
    public void testTryWrite() {
        sessionInit.tryWrite(new UnpooledByteBufAllocator(true).buffer());
    }

    @Test
    public void testConnect() {
        sessionInit.connect();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDisconnect() {
        sessionInit.disconnect();
    }

    @Test
    public void testName() {
        Assert.assertEquals(sessionInit.toString(), sessionInit.name());
    }
}