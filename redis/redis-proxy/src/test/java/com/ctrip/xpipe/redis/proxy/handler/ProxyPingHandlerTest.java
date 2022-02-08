package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.handler.response.ProxyPingHandler;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.resource.TestResourceManager;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ProxyPingHandlerTest extends AbstractProxyIntegrationTest {

    private ProxyPingHandler handler;

    private ResourceManager manager;

    @Before
    public void before() {
        manager = new TestResourceManager();
        handler = new ProxyPingHandler(manager);
    }

    @Test
    public void testGetOption() {
        Assert.assertEquals(PROXY_OPTION.PING, handler.getOption());
    }

    @Test
    public void testDoHandle() {
        Channel channel = mock(Channel.class);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", randomPort());
        when(channel.localAddress()).thenReturn(addr);
        AtomicReference<String> actual = new AtomicReference<>();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                actual.set(new SimpleStringParser().read(invocation.getArgument(0, ByteBuf.class)).getPayload());
                return null;
            }
        }).when(channel).writeAndFlush(any(ByteBuf.class));
        handler.handle(channel, new String[]{"PING"});
        String expected = "PROXY PONG 127.0.0.1:" + addr.getPort();
        Assert.assertEquals(expected, actual.get());
    }

    @Test
    public void testDoHandleWithForwardPing() throws Exception {
        int port = randomPort();
        Server server = startServer(port, "+PROXY PONG 127.0.0.1:" + port + "\r\n");
        Channel channel = mock(Channel.class);
        AtomicReference<String> actual = new AtomicReference<>();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                actual.set(new SimpleStringParser().read(invocation.getArgument(0, ByteBuf.class)).getPayload());
                return null;
            }
        }).when(channel).writeAndFlush(any(ByteBuf.class));

        Endpoint key = new DefaultProxyEndpoint("TCP://127.0.0.1:" + server.getPort());
        SimpleObjectPool<NettyClient> pool = manager.getKeyedObjectPool().getKeyPool(key);
        NettyClient client = pool.borrowObject();
        pool.returnObject(client);

        handler.handle(channel, new String[]{"PING", "TCP://127.0.0.1:" + server.getPort()});

        String expected = String.format("PROXY PONG 127.0.0.1:%d 127.0.0.1:%d", server.getPort(), server.getPort());
        waitConditionUntilTimeOut(()->actual.get() != null, 1000);

        logger.info("[receive] {}", actual.get());
        Assert.assertTrue(actual.get().startsWith(expected));
        server.stop();
    }
}