package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.keeper.command.AbstractKeeperCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import jakarta.annotation.PostConstruct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.when;


@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class AbstractKeeperCommandTest {

    @Mock
    XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Mock
    ScheduledExecutorService scheduled;

    @Mock
    SimpleObjectPool<NettyClient> keyPool;

    @Mock
    InfoCommand infoCommand;

    @Mock
    CommandFuture<String> infoCommandFuture;

    static Endpoint key = new DefaultEndPoint("10.10.10.10", 6379);

    @PostConstruct
    public void post() {
        when(infoCommand.execute()).thenReturn(infoCommandFuture);
        when(keyedObjectPool.getKeyPool(key)).thenReturn(keyPool);
    }

    @Test
    public void testGetKeeperCommandName() throws Throwable {
        TestAbstractKeeperCommandTest test = new TestAbstractKeeperCommandTest(keyedObjectPool, scheduled);
        test.doExecute();
    }

    private static class TestAbstractKeeperCommandTest<T> extends AbstractKeeperCommand<T>{

        protected TestAbstractKeeperCommandTest(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled) {
            super(keyedObjectPool, scheduled);
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        protected void doExecute() throws Throwable {
            this.generateInfoReplicationCommand(key);
        }

        @Override
        protected void doReset() {

        }
    }

}
