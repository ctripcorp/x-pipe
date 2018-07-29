package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 22, 2018
 */
public class FrontendSessionHandlerTest extends AbstractRedisProxyServerTest {

    private FrontendSessionNettyHandler handler = new FrontendSessionNettyHandler(tunnelManager());

    @Test
    public void testFormatByteBuf() {
        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer();
        byteBuf.writeBytes("Test Byte Buf Message".getBytes());

        String message = handler.formatByteBuf("TEST", byteBuf);
        logger.info(message);
    }
}