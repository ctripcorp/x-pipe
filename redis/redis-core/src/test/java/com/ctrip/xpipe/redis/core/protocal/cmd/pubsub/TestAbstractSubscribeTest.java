package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;


import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * @author chen.zhu
 * <p>
 * Apr 09, 2018
 */
public class TestAbstractSubscribeTest extends AbstractRedisTest {

    private AbstractSubscribe subscribe;

    private String channel;

    private int port;

    private Server server;

    @Before
    public void beforeAbstractSubscribeTest() throws Exception {
        port = randomPort();
        channel = "Hello";
        subscribe = new AbstractSubscribe("127.0.0.1", port, scheduled, Subscribe.MESSAGE_TYPE.MESSAGE, channel) {
            @Override
            protected void doUnsubscribe() {

            }

            @Override
            protected SubscribeMessageHandler getSubscribeMessageHandler() {
                return new DefaultSubscribeMessageHandler();
            }
        };

        server = startServer(port, "+OK");
    }

    @After
    public void afterAbstractSubscribeTest() throws Exception {
        server.stop();
    }

    @Test(expected = RedisRuntimeException.class)
    public void testHandleResponse1() {
        subscribe.handleResponse(null, "error");
    }

    @Test(expected = RedisRuntimeException.class)
    public void testHandleResponse2() throws IOException {
        Object[] response = new Object[]{new ByteArrayOutputStreamPayload("psubscribe"), new ByteArrayOutputStreamPayload("hello*"), (Long) 1L};
        subscribe.handleResponse(null, response);
    }

    @Test(expected = RedisRuntimeException.class)
    public void testHandleResponse3() throws IOException {
        Object[] response = new Object[]{new ByteArrayOutputStreamPayload("subscribe"), new ByteArrayOutputStreamPayload(channel)};
        subscribe.handleResponse(null, response);
    }

    @Test
    public void testHandleResponse4() throws IOException {
        Object[] response = new Object[]{new ByteArrayOutputStreamPayload(Subscribe.SUBSCRIBE), new ByteArrayOutputStreamPayload("channel"), (Long) 2L};
        subscribe.handleResponse(null, response);
    }

    @Test
    public void testHandleResponse5() throws Exception {
        Object[] response = new Object[]{new ByteArrayOutputStreamPayload(Subscribe.SUBSCRIBE), new ByteArrayOutputStreamPayload(channel), (Long) 2L};
        Channel nettyChannel = getXpipeNettyClientKeyedObjectPool().borrowObject(new DefaultEndPoint("127.0.0.1", port)).channel();
        subscribe.handleResponse(nettyChannel, response);
        Assert.assertEquals(Subscribe.SUBSCRIBE_STATE.SUBSCRIBING, subscribe.getSubscribeState());
    }

    @Test
    public void testHandleMessage() throws Exception {
        String message = "message";
        Object[] response = new Object[]{new ByteArrayOutputStreamPayload(Subscribe.MESSAGE_TYPE.MESSAGE.desc()),
                new ByteArrayOutputStreamPayload(channel), new ByteArrayOutputStreamPayload(message)};

        subscribe.addChannelListener((ch, msg) -> {Assert.assertEquals(channel, ch); Assert.assertEquals(message, msg);});
        subscribe.handleMessage(response);
    }

    @Test
    public void testNoDupNettyClient() throws Exception {
        Server server = startEmptyServer();
        AtomicReference<NettyClient> clientReference = new AtomicReference<>();
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool().
                getKeyPool(new DefaultEndPoint("localhost", server.getPort()));
        int N = 5;
        for(int i = 0; i < N; i++) {
            AbstractSubscribe command = new TestNettyClientNoDupCommand(clientPool, scheduled, clientReference,
                    (nettyClient, reference)->Assert.assertNotEquals(nettyClient, reference.getAndSet(nettyClient)));
            command.execute();
        }
        server.stop();
    }

    @Test
    public void testSubscribeWithCRLFBegin() throws Exception {
        String message = "message";
        Object[] response = new Object[]{new ByteArrayOutputStreamPayload(Subscribe.MESSAGE_TYPE.MESSAGE.desc()),
                new ByteArrayOutputStreamPayload(channel), new ByteArrayOutputStreamPayload(message)};
        subscribe.setSubscribeState(Subscribe.SUBSCRIBE_STATE.SUBSCRIBING);
        subscribe.addChannelListener((ch, msg) -> {Assert.assertEquals(channel, ch); Assert.assertEquals(message, msg);});
        ByteBuf byteBuf = new ArrayParser(response).format();
        CompositeByteBuf result = (CompositeByteBuf) byteBuf;
        result.addComponent(0, Unpooled.copiedBuffer("\r\n".getBytes()));
        subscribe.doReceiveResponse(new EmbeddedChannel(), byteBuf);
    }

    @Test
    public void testSubscribeWithEmptyByteBuf() throws Exception {
        Assert.assertNull(subscribe.doReceiveResponse(new EmbeddedChannel(), Unpooled.buffer()));
    }

    class TestNettyClientNoDupCommand extends AbstractSubscribe {

        private AtomicReference<NettyClient> clientReference;
        private BiConsumer<NettyClient, AtomicReference<NettyClient>> consumer;

        public TestNettyClientNoDupCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                                                AtomicReference<NettyClient> clientReference,
                                                BiConsumer<NettyClient, AtomicReference<NettyClient>> consumer) {
            super(clientPool, scheduled, MESSAGE_TYPE.MESSAGE, "test");
            this.clientReference = clientReference;
            this.consumer = consumer;
        }

        @Override
        protected void doSendRequest(NettyClient nettyClient, ByteBuf byteBuf) {
            consumer.accept(nettyClient, clientReference);
        }

        @Override
        protected void doUnsubscribe() {

        }

        @Override
        protected SubscribeMessageHandler getSubscribeMessageHandler() {
            return new DefaultSubscribeMessageHandler();
        }
    }

}