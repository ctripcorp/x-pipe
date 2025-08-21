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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.pool2.PooledObject;
import org.junit.*;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    private static final int CHECK_INTERVAL = 1000;

    private Server server;

    private Endpoint endpoint;

    @BeforeClass
    public static void beforeNettyClientFactoryTestClass() {
        System.setProperty("io.netty.allocator.useCacheForAllThreads", "false");
    }

    @Before
    public void beforeNettyClientFactoryTest() throws Exception {
        if(server == null) {
            server = startServer("+OK\r\n");
            endpoint = new DefaultEndPoint(LOCAL_HOST, server.getPort());
        }
        objectPool = new XpipeNettyClientKeyedObjectPool(factory);
        objectPool.initialize();
        objectPool.start();
    }

    @Test
    public void testMakeObjectWithFastThreadLocalCache() throws Exception {

        PooledObject<NettyClient> pooledObject = factory.makeObject(endpoint);
        NettyClient client = pooledObject.getObject();
        OkCommand command = new OkCommand(objectPool.getKeyPool(endpoint), scheduled, CHECK_INTERVAL);
        command.execute().get();

        PooledByteBufAllocator allocator = (PooledByteBufAllocator)client.channel().alloc();
        AtomicReference<Object> freeSweepAllocationThreshold = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client.channel().eventLoop().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Field field = FieldUtils.getDeclaredField(allocator.getClass(), "threadCache", true);
                    Object threadCache = field.get(allocator);
                    logger.info("[class]{}", threadCache.getClass());
                    FastThreadLocal fastThreadLocal = (FastThreadLocal) threadCache;

                    Object poolThreadCache = fastThreadLocal.get();
                    logger.info("[class]{}", poolThreadCache.getClass());
                    Field param = FieldUtils.getField(poolThreadCache.getClass(), "freeSweepAllocationThreshold", true);
                    logger.info("[{}]", param.get(poolThreadCache));
                    freeSweepAllocationThreshold.set(param.get(poolThreadCache));
                } catch (IllegalAccessException e) {
                    logger.error("", e);
                }
                latch.countDown();
            }
        });
        latch.await();
        Assert.assertNotNull(freeSweepAllocationThreshold.get());
        Assert.assertNotEquals(0, freeSweepAllocationThreshold.get());
    }

    //manual
    @Ignore
    @Test
    public void preTest() throws Exception {
        Server server = startServer("+OK\r\n");
        Endpoint endpoint = new DefaultEndPoint(LOCAL_HOST, server.getPort());
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

    //manual
    @Ignore
    @Test
    public void testTimeoutWhenBorrow() {
        PingCommand command = new PingCommand(objectPool.getKeyPool(new DefaultEndPoint(getTimeoutIp(), randomPort())), scheduled, CHECK_INTERVAL);
        command.execute();
        sleep(1000 * 10);
    }

    //manual
    @Ignore
    @Test
    public void testMakeObjectManual() throws Exception {
        Server server = startServer("+OK\r\n");
        Endpoint endpoint = new DefaultEndPoint(LOCAL_HOST, server.getPort());
        AtomicBoolean result = new AtomicBoolean(true);

        AtomicLong lastUpdateMilli = new AtomicLong(-1);
        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                PingCommand command = new PingCommand(objectPool.getKeyPool(new DefaultEndPoint(getTimeoutIp(), randomPort())), scheduled, CHECK_INTERVAL);
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
                command.execute(executors);
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