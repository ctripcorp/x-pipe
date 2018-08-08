package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;


import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * Apr 09, 2018
 */
public class AbstractSubscribeTest extends AbstractRedisTest {

    private AbstractSubscribe subscribe;

    private String channel;

    private int port;

    @Before
    public void beforeAbstractSubscribeTest() throws Exception {
        port = randomPort();
        channel = "Hello";
        subscribe = new AbstractSubscribe("127.0.0.1", port, scheduled, channel,
                Subscribe.MESSAGE_TYPE.MESSAGE) {
            @Override
            protected void doUnsubscribe() {

            }
        };

        startServer(port, "+OK");
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

    @Test(expected = RedisRuntimeException.class)
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
}