package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.session.DefaultSession;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class SessionClosingTest extends AbstractRedisProxyServerTest {

    private SessionState seesionClosing;

    private DefaultSession frontend;

    @Before
    public void beforeSessionClosedTest() throws Exception {
        frontend = (DefaultSession) frontend();
        seesionClosing = new SessionClosing(frontend);
    }

    @Test
    public void testDoNextAfterSuccess() {
        Assert.assertEquals(new SessionClosed(frontend), seesionClosing.nextAfterSuccess());
    }

    @Test
    public void testDoNextAfterFail() {
        Assert.assertEquals(new SessionClosing(frontend), seesionClosing.nextAfterFail());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testTryWrite() {
        ByteBuf byteBuf = new UnpooledByteBufAllocator(false).buffer();
        byteBuf.setBytes(0, "+OK\r\n".getBytes());
        seesionClosing.tryWrite(byteBuf);
        ReferenceCountUtil.release(byteBuf);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnect() {
        seesionClosing.connect();
    }

    @Test
    public void testDisconnect() {
//        seesionClosing.disconnect();
    }

    @Test
    public void testName() {
        Assert.assertEquals(seesionClosing.toString(), seesionClosing.name());
    }

}