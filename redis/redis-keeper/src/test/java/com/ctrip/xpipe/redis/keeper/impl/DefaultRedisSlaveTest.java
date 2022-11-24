package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisSingleKeyOpGtidWrapper;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
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
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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

    @Mock
    private ReplicationStore replicationStore;

    @Mock
    private KeeperRepl keeperRepl;

    public DefaultRedisSlave redisSlave;

    @Before
    public void beforeDefaultRedisSlaveTest() {

        when(channel.closeFuture()).thenReturn(new DefaultChannelPromise(channel));
        when(channel.remoteAddress()).thenReturn(localhostInetAdress(randomPort()));
        when(keeperRepl.replId()).thenReturn("test-repl-id");
        when(redisKeeperServer.getKeeperRepl()).thenReturn(keeperRepl);

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
        shouldThrowException(() -> redisSlave.onCommand(null, 0L, mock(ReferenceFileRegion.class)));
        shouldThrowException(() -> redisSlave.beginWriteRdb(mock(EofType.class), new OffsetReplicationProgress(0L)));
        shouldThrowException(() -> redisSlave.beginWriteCommands(new OffsetReplicationProgress(0L)));

        redisSlave.markPsyncProcessed();
        //all should fail
        shouldThrowException(() -> redisSlave.sendMessage(randomString(10).getBytes()));
        shouldThrowException(() -> redisSlave.sendMessage(Unpooled.wrappedBuffer(randomString(10).getBytes())));
        shouldThrowException(() -> redisSlave.onCommand(null, 0L, mock(ReferenceFileRegion.class)));
        shouldThrowException(() -> redisSlave.beginWriteRdb(mock(EofType.class), new OffsetReplicationProgress(0L)));
        shouldThrowException(() -> redisSlave.beginWriteCommands(new OffsetReplicationProgress(0L)));

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

    @Test
    public void testConcurrentCloseAndMarkPsyncProcessed() throws IOException, InterruptedException {

        int rount = 10;
        final AtomicReference<Exception>  exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(rount*2);

        for(int i=0;i<rount;i++) {

            RedisClient redisClient = new DefaultRedisClient(channel, redisKeeperServer);
            redisSlave= new DefaultRedisSlave(redisClient);

            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        redisSlave.close(50);
                    } catch (Exception e) {
                        logger.error("error close slave", e);
                        exception.set(e);
                    }finally {
                        latch.countDown();
                    }
                }
            });

            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        sleep(3);
                        redisSlave.markPsyncProcessed();
                    }finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        Assert.assertNull(exception.get());
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

        redisSlave.beginWriteRdb(new LenEofType(1000), new OffsetReplicationProgress(2));

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

    @Test
    public void testSlaveClosedWhenSendCommandFail() throws Exception {
        when(redisKeeperServer.getReplicationStore()).thenReturn(replicationStore);
        doThrow(new IOException("File for offset 0 does not exist")).when(replicationStore).addCommandsListener(any(), any());

        redisSlave.beginWriteRdb(new LenEofType(1000), new OffsetReplicationProgress(0));
        Assert.assertTrue(redisSlave.isOpen());
        redisSlave.sendCommandForFullSync();
        waitConditionUntilTimeOut(() -> !redisSlave.isOpen());
    }

    @Test
    public void testMultiBeginWritingCmds() throws Exception {
        when(redisKeeperServer.getReplicationStore()).thenReturn(replicationStore);
        redisSlave.beginWriteCommands(new OffsetReplicationProgress(0L));
        redisSlave.beginWriteCommands(new OffsetReplicationProgress(0L));

        verify(replicationStore).addCommandsListener(any(), any());
    }

    @Test
    public void testFilterPublish() {
        RedisSingleKeyOp op1 = new RedisOpSingleKey(RedisOpType.SET, string2Bytes("set a 1"), null, null);
        RedisOp gtidOp1 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op1);
        Assert.assertFalse(redisSlave.shouldFilter(gtidOp1));

        RedisSingleKeyOp op2 = new RedisOpSingleKey(RedisOpType.PUBLISH, string2Bytes("publish xpipe-hetero-ppp 222"), null, null);
        RedisOp gtidOp2 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op2);
        Assert.assertFalse(redisSlave.shouldFilter(gtidOp2));

        RedisSingleKeyOp op3 = new RedisOpSingleKey(RedisOpType.PUBLISH, string2Bytes("publish ppp 222"), null, null);
        RedisOp gtidOp3 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op3);
        Assert.assertTrue(redisSlave.shouldFilter(gtidOp3));
        Assert.assertTrue(redisSlave.shouldFilter(op3));
    }

    private byte[][] string2Bytes(String s) {

        String[] ss = s.split(" ");
        int length = ss.length;
        byte[][] b = new byte[length][];

        for (int i = 0; i < length; i++) {
            b[i] = ss[i].getBytes();
        }

        return b;
    }
}
