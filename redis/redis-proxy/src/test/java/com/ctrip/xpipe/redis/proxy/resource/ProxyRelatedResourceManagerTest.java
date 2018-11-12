package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.proxy.AbstractProxySpringEnabledTest;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class ProxyRelatedResourceManagerTest extends AbstractProxySpringEnabledTest {

    @Autowired
    private ResourceManager resourceManager;

    @Ignore
    @Test
    public void testGetKeyedObjectPool() throws Exception {
        Server server = startEchoServer();
        SimpleKeyedObjectPool<Endpoint, NettyClient> keyedObjectPool = resourceManager.getKeyedObjectPool();
        SimpleObjectPool<NettyClient> objectPool = keyedObjectPool.getKeyPool(localhostEndpoint(server.getPort()));
        NettyClient nettyClient = objectPool.borrowObject();
        nettyClient.sendRequest(Unpooled.copiedBuffer("hello world".getBytes()), new ByteBufReceiver() {
            @Override
            public RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf) {
                logger.info("[receive]{}", ByteBufUtils.readToString(byteBuf));
                return null;
            }

            @Override
            public void clientClosed(NettyClient nettyClient) {

            }
        });
        sleep(1000 * 10);
    }
}