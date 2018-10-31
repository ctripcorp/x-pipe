package com.ctrip.xpipe.redis.core.proxy.command;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class ProxyPingCommandTest extends AbstractRedisTest {

    private Server server;

    @Before
    public void beforeProxyPingCommandTest() throws Exception {
        int port = randomPort();
        server = startServer(port, "+PROXY PONG 127.0.0.1:" + port + "\r\n");
    }

    @After
    public void afterProxyPingCommandTest() throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    @Test
    public void testSimplePing() throws Exception {
        CommandFuture<ProxyPongEntity> future = new ProxyPingCommand(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(localhostEndpoint(server.getPort())),
                scheduled).execute();
        ProxyPongEntity pong = future.get();
        Assert.assertEquals(new ProxyPongEntity(new HostPort("127.0.0.1", server.getPort())), pong);
    }
}