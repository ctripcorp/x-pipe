package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisClient;
import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class ProxyCommandHandlerTest {

    private ProxyCommandHandler handler = new ProxyCommandHandler();

    private Channel channel = new NioSocketChannel();

    @Test
    public void testDoHandle() {
        final String command = "ROUTE proxy://127.0.0.1:6379,tls://10.2.1.1:6379" +
                " tls://10.3.2.1:6379,tls://10.3.2.1:6380 raw://127.0.0.1:6380;FORWARD_FOR 192.168.1.1:6379";

        DefaultRedisClient client = new DefaultRedisClient(channel, null);
        handler.doHandle(command.split(WHITE_SPACE), client);
        Assert.assertEquals("192.168.1.1", client.ip());
    }
}