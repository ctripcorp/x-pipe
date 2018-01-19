package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 13, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisSlaveTest extends AbstractRedisKeeperTest {

    private int waitDumpMilli = 100;

    @Mock
    public Channel channel;

    @Mock
    public RedisKeeperServer redisKeeperServer;

    public DefaultRedisSlave redisSlave;

    @Before
    public void beforeDefaultRedisSlaveTest() {

        when(channel.closeFuture()).thenReturn(new DefaultChannelPromise(channel));
        when(channel.remoteAddress()).thenReturn(localhostInetAddress(randomPort()));

        RedisClient redisClient = new DefaultRedisClient(channel, redisKeeperServer);
        redisSlave= new DefaultRedisSlave(redisClient);

        redisSlave.setRdbDumpMaxWaitMilli(waitDumpMilli);

    }

    @Test
    public void testClose() throws IOException {

        redisSlave.close();

        //should success
        redisSlave.sendMessage(randomString(10).getBytes());
        redisSlave.sendMessage(Unpooled.wrappedBuffer(randomString(10).getBytes()));

        //should fail
        shouldThrowException(() -> redisSlave.onCommand(mock(ReferenceFileRegion.class)));
        shouldThrowException(() -> redisSlave.beginWriteRdb(mock(EofType.class), 0L));
        shouldThrowException(() -> redisSlave.beginWriteCommands(0L));

        redisSlave.markPsyncProcessed();
        //all should fail
        shouldThrowException(() -> redisSlave.sendMessage(randomString(10).getBytes()));
        shouldThrowException(() -> redisSlave.sendMessage(Unpooled.wrappedBuffer(randomString(10).getBytes())));
        shouldThrowException(() -> redisSlave.onCommand(mock(ReferenceFileRegion.class)));
        shouldThrowException(() -> redisSlave.beginWriteRdb(mock(EofType.class), 0L));
        shouldThrowException(() -> redisSlave.beginWriteCommands(0L));

    }

    @Test
    public void testDoRealCloseTimeout() throws IOException, TimeoutException {

        int timeoutMilli = 100;

        redisSlave.setWaitForPsyncProcessedTimeoutMilli(timeoutMilli);
        redisSlave.close();
        Assert.assertTrue(redisSlave.getCloseState().isClosing());
        waitConditionUntilTimeOut(() -> redisSlave.getCloseState().isClosed());
    }


    @Test
    public void testCloseTimeoutNotMarkPsyncProcessed() throws IOException {

        redisSlave.markPsyncProcessed();

        Assert.assertTrue(redisSlave.getCloseState().isOpen());

        redisSlave.close();
        Assert.assertTrue(redisSlave.getCloseState().isClosed());
    }




    @SuppressWarnings("resource")
    @Test
    public void testWaitRdbTimeout() {

        redisSlave.waitForRdbDumping();
        sleep(waitDumpMilli * 2);
        Assert.assertTrue(!redisSlave.isOpen());

    }

    @SuppressWarnings("resource")
    @Test
    public void testWaitRdbNormal() {

        redisSlave.waitForRdbDumping();

        sleep(waitDumpMilli / 2);

        redisSlave.beginWriteRdb(new LenEofType(1000), 2);

        sleep(waitDumpMilli);

        Assert.assertTrue(redisSlave.isOpen());

    }

    @Test
    public void testFuture() {

        SettableFuture<Boolean> objectSettableFuture = SettableFuture.create();
        AtomicInteger listenerCount = new AtomicInteger(0);
        AtomicInteger notifyCount = new AtomicInteger();

        executors.execute(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {

                while (!Thread.interrupted()) {

                    listenerCount.incrementAndGet();
                    objectSettableFuture.addListener(new Runnable() {
                        @Override
                        public void run() {
                            notifyCount.incrementAndGet();
                        }
                    }, MoreExecutors.directExecutor());
                }

                logger.info("exit thread");
            }
        });

        sleep(10);
        objectSettableFuture.set(true);

        executors.shutdownNow();
        sleep(10);

        logger.info("{}, {}", listenerCount, notifyCount);
        Assert.assertEquals(listenerCount.get(), notifyCount.get());
    }

}
