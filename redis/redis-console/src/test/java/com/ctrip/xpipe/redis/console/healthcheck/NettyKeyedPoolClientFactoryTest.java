package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.DateTimeUtils;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.OK;
import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 27, 2018
 */
public class NettyKeyedPoolClientFactoryTest extends AbstractRedisTest {

    private NettyKeyedPoolClientFactory factory = new NettyKeyedPoolClientFactory(1);

    private XpipeNettyClientKeyedObjectPool objectPool;

    private static final int CHECK_INTERVAL = 100;

    @Before
    public void beforeNettyKeyedPoolClientFactoryTest() throws Exception {
        objectPool = new XpipeNettyClientKeyedObjectPool(factory);
        objectPool.initialize();
        objectPool.start();
    }

    @Test
    public void preTest() throws Exception {
        Server server = startServer("+OK\r\n");
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());
        OkCommand command = new OkCommand(objectPool.getKeyPool(endpoint), scheduled, CHECK_INTERVAL);
        command.future().addListener(new CommandFutureListener<String>() {
            @Override
            public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                logger.info("[operationComplete] {}", commandFuture.get());
            }
        });
        command.execute();
        sleep(100);
    }

    @Test
    public void testMakeObject() throws Exception {
        Server server = startServer("+OK\r\n");
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());
        AtomicLong lastUpdateMilli = new AtomicLong(-1);
        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                PingCommand command = new PingCommand(objectPool.getKeyPool(new DefaultEndPoint("127.0.0.1", randomPort())), scheduled, CHECK_INTERVAL);
                command.execute();
            }
        }, randomInt(1000, 2000), CHECK_INTERVAL, TimeUnit.MILLISECONDS);
        for(int i = 0; i < 2000 / CHECK_INTERVAL + 100; i ++) {
            OkCommand command = new OkCommand(objectPool.getKeyPool(endpoint), scheduled, CHECK_INTERVAL);
            command.future().addListener(new CommandFutureListener<String>() {
                @Override
                public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                    long last = lastUpdateMilli.getAndSet(System.currentTimeMillis());
                    checkout(last);
                }
            });
            command.execute();
            sleep(CHECK_INTERVAL);
        }
    }

    private void checkout(long lastUpdate) {
        if(lastUpdate > 0) {
            long delta = System.currentTimeMillis() - lastUpdate;
            logger.info("[delta] {}", delta);
            if(delta > CHECK_INTERVAL + CHECK_INTERVAL/2) {
                throw new IllegalStateException("last time update: " + DateTimeUtils.timeAsString(lastUpdate));
            }
        }
    }

    class OkCommand extends AbstractRedisCommand<String> {

        public OkCommand(SimpleObjectPool<NettyClient> clientPool,
                         ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
        }

        public OkCommand(SimpleObjectPool<NettyClient> clientPool,
                         ScheduledExecutorService scheduled,
                         int commandTimeoutMilli) {
            super(clientPool, scheduled, commandTimeoutMilli);
        }

        @Override
        protected String format(Object payload) {
            return "OK";
        }

        @Override
        public ByteBuf getRequest() {
            return new RequestStringParser(OK).format();
        }
    }
}