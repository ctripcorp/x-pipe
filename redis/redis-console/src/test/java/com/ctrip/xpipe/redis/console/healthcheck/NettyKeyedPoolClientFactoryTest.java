package com.ctrip.xpipe.redis.console.healthcheck;

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
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.OK;

/**
 * @author chen.zhu
 * <p>
 * Sep 27, 2018
 */
public class NettyKeyedPoolClientFactoryTest extends AbstractRedisTest {

    private NettyKeyedPoolClientFactory factory = new NettyKeyedPoolClientFactory(1);

    private XpipeNettyClientKeyedObjectPool objectPool;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("scheduled"));

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
    public void testTimeoutWhenBorrow() {
        PingCommand command = new PingCommand(objectPool.getKeyPool(new DefaultEndPoint("10.0.0.1", randomPort())), scheduled, CHECK_INTERVAL);
        command.execute();
        sleep(1000 * 10);
    }

    @Test
    public void testMakeObject() throws Exception {
        Server server = startServer("+OK\r\n");
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", server.getPort());
        AtomicBoolean result = new AtomicBoolean(true);

        AtomicLong lastUpdateMilli = new AtomicLong(-1);
        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                PingCommand command = new PingCommand(objectPool.getKeyPool(new DefaultEndPoint("10.0.0.1", randomPort())), scheduled, CHECK_INTERVAL);
                command.execute();
            }
        }, randomInt(1000, 2000), CHECK_INTERVAL, TimeUnit.MILLISECONDS);
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                OkCommand command = new OkCommand(objectPool.getKeyPool(endpoint), scheduled, CHECK_INTERVAL);
                command.future().addListener(new CommandFutureListener<String>() {
                    @Override
                    public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                        long last = lastUpdateMilli.getAndSet(System.currentTimeMillis());
                        try {
                            checkout(last);
                        } catch (Exception e) {
                            logger.error("[checkout]", e);
                            result.set(false);
                        }
                    }
                });
                command.execute();
            }
        }, 0, CHECK_INTERVAL, TimeUnit.MILLISECONDS);

        sleep(10 * 1000);
        Assert.assertTrue(result.get());
    }

    private void checkout(long lastUpdate) {
        if(lastUpdate > 0) {
            long delta = System.currentTimeMillis() - lastUpdate;
            logger.info("[delta] {}", delta);
            if(delta > CHECK_INTERVAL + 200) {
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