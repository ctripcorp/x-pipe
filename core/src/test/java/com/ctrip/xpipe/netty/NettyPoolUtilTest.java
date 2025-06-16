package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.xpipe.AbstractTest.LOCAL_HOST;
import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 15, 2020
 */
public class NettyPoolUtilTest extends AbstractTest {

    @Test
    public void testCreateNettyPool() {
    }

    @Test
    public void testMakeObjectNoTooMuchEventLoops() throws Exception {
        int tasks = 100;
        Server server = startServer("+PONG");
        int beforeSize = Thread.getAllStackTraces().size();
        for(int i = 0; i < tasks; i++) {
            SimpleObjectPool<NettyClient> pool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(LOCAL_HOST, server.getPort()));
            pool.borrowObject().sendRequest(Unpooled.copiedBuffer("+PING\r\n".getBytes()));
        }
        int afterSize = Thread.getAllStackTraces().size();
        logger.info("[delta size] {}", afterSize - beforeSize);
        Assert.assertTrue(afterSize - beforeSize < tasks);
    }
}