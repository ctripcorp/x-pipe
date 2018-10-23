package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisClient;
import com.ctrip.xpipe.redis.keeper.impl.fakeredis.AbstractFakeRedisTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Queue;

/**
 * @author chen.zhu
 * <p>
 * May 31, 2018
 */
public class InfoHandlerTest extends AbstractFakeRedisTest {

    private RedisKeeperServer keeperServer;

    @Before
    public void beforeInfoHandlerTest() throws Exception {
        keeperServer = startRedisKeeperServerAndConnectToFakeRedis();
    }

    @Ignore
    @Test
    public void testInfoStatsManually() throws Exception {
        int port = keeperServer.getListeningPort();
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", port));
        socket.getOutputStream().write("info stats".getBytes());
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        new InfoHandler().doHandle(new String[]{"stats"}, new DefaultRedisClient(new EmbeddedChannel(), keeperServer));
        String line = null;
        logger.info("listening-port: {}", port);
        while((line = reader.readLine()) != null) {
            logger.info("{}", line);
        }
    }

    @Test
    public void testInfoAll() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        new InfoHandler().handle(new String[]{"all"}, new DefaultRedisClient(channel, keeperServer));
        Assert.assertFalse(channel.outboundMessages().isEmpty());
    }

    @Test
    public void testInfoStats() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        new InfoHandler().handle(new String[]{"Stats"}, new DefaultRedisClient(channel, keeperServer));
        Assert.assertFalse(channel.outboundMessages().isEmpty());
    }

    @Test
    public void testGetHeader() {
        String header = String.format("# %s%s", name(), RedisProtocol.CRLF);
        logger.info("[testGetHeader] {}", header);
        Assert.assertEquals("# Stats\r\n", header);
    }

    private String name() {
        return "Stats";
    }
}