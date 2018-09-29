package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 11, 2018
 */
public class TestAbstractSentinelCommandTest extends AbstractRedisTest {

    private SimpleObjectPool<NettyClient> clientPool;

    @Before
    public void beforeAbstractSentinelCommandTest() throws Exception {
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", 5000);
        clientPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(endpoint);
    }

    @Test
    public void testMonitorMaster() throws Exception {
        Command command = new AbstractSentinelCommand
                .SentinelMonitor(clientPool, scheduled, "test", new HostPort("127.0.0.1", 6379), 3);
        command.execute();
        Thread.sleep(100);
    }

    @Test
    public void testSentinelReset() throws Exception {
        Command command = new AbstractSentinelCommand.SentinelReset(clientPool, "test", scheduled);
        command.execute();
        Thread.sleep(1000);
    }

    @Test
    public void testSentinelSlaves() throws Exception {
        Command<List<HostPort>> command = new AbstractSentinelCommand.SentinelSlaves(clientPool, scheduled, "test");
        List<HostPort> slaves = command.execute().get();
        logger.info("[slaves] {}", slaves);
    }

}