package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.command.AbstractProxyCommand;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

public class ProxyProtocolDecoderTest extends AbstractProxyIntegrationTest {

    private ProxyProtocolDecoder decoder = new ProxyProtocolDecoder(128);

    private EmbeddedChannel channel = new EmbeddedChannel(decoder);


    /**
     * Special test for decoder, found that if we make singleDecode() return true
     * Things went wrong for Request/Response mode;
     * It(the test case) would throw Timeout exception in not-fixed version*/
    @Test
    public void testDecodeOnlyOnce() throws Exception {
        startFirstProxy();
        SimpleObjectPool<NettyClient> objectPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultProxyEndpoint("127.0.0.1", FIRST_PROXY_TCP_PORT));
        NettyClient client = objectPool.borrowObject();
        waitConditionUntilTimeOut(()->client.channel().isActive(), 1000);
        SimpleObjectPool<NettyClient> pool = new FixedObjectPool<>(client);

        Command<Object> task = new AbstractProxyCommand<Object>(pool, scheduled) {
            @Override
            protected Object format(Object payload) {
                return payload;
            }

            @Override
            public ByteBuf getRequest() {
                ByteBuf byteBuf1 = new SimpleStringParser("PROXY MONITOR PingStats").format();
                ByteBuf byteBuf2 = new SimpleStringParser("PROXY MONITOR TunnelStats").format();
                ByteBuf byteBuf3 = new SimpleStringParser("PROXY MONITOR SocketStats").format();
                ByteBuf byteBuf = Unpooled.copiedBuffer(byteBuf1, byteBuf2, byteBuf3);
                return byteBuf;
            }
        };
        ((AbstractProxyCommand<Object>) task).setCommandTimeoutMilli(500);

        Object obj = task.execute().get();

        sleep(1000);
    }
}
