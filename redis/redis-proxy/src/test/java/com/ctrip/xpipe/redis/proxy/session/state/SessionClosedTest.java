package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.exception.WriteToClosedSessionException;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class SessionClosedTest extends AbstractRedisProxyServerTest {

    private SessionState sessionClosed;

    private Session frontend;

    @Before
    public void beforeSessionClosedTest() throws Exception {
        frontend = frontend();
        sessionClosed = new SessionClosed(frontend);
    }

    @Test
    public void testDoNextAfterSuccess() {
        Assert.assertTrue(sessionClosed == sessionClosed.nextAfterSuccess());
    }

    @Test
    public void testDoNextAfterFail() {
        Assert.assertEquals(new SessionClosed(frontend), sessionClosed.nextAfterFail());
    }

    @Test(expected = WriteToClosedSessionException.class)
    public void testTryWrite() {
        ByteBuf byteBuf = new UnpooledByteBufAllocator(false).buffer();
        byteBuf.setBytes(0, "+OK\r\n".getBytes());
        try {
            sessionClosed.tryWrite(byteBuf);
        } catch (Exception e) {
            throw e;
        } finally {
            ReferenceCountUtil.release(byteBuf);
        }

    }

    @Test
    public void testName() {
        Assert.assertEquals(sessionClosed.toString(), sessionClosed.name());
    }

}