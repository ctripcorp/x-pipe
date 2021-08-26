package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

/**
 * @author lishanglin
 * date 2021/8/23
 */
@RunWith(MockitoJUnitRunner.class)
public class PsyncKeeperServerStateObserverTest extends AbstractRedisKeeperTest {

    private RedisKeeperServerStateBackup.PsyncKeeperServerStateObserver observer;

    @Mock
    private RedisClient redisClient;

    @Mock
    private RedisKeeperServer redisKeeperServer;

    @Mock
    private ReplicationStore replicationStore;

    private ThreadPoolExecutor singleThreadExecutors;

    @Before
    public void beforePsyncKeeperServerStateObserverTest() {
        singleThreadExecutors = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), XpipeThreadFactory.create("single-thread-test-executors"));

        observer = Mockito.spy(new RedisKeeperServerStateBackup.PsyncKeeperServerStateObserver(new String[]{"?", "-1"}, redisClient));
        when(redisClient.getRedisKeeperServer()).thenReturn(redisKeeperServer);
        when(redisKeeperServer.getReplicationStore()).thenReturn(replicationStore);
        when(replicationStore.isFresh()).thenReturn(true);
        doAnswer(invocation -> {
            Runnable task = invocation.getArgumentAt(0, Runnable.class);
            singleThreadExecutors.execute(task);
            return null;
        }).when(redisKeeperServer).processCommandSequentially(Mockito.any());
    }

    @Test
    public void testMultiThreadNotifyObserver() throws Exception {
        int multi = 100;
        CountDownLatch latch = new CountDownLatch(multi);

        IntStream.range(0, multi).forEach(i -> {
            executors.execute(() -> {
                try {
                    observer.update(mock(KeeperServerStateChanged.class), mock(Observable.class));
                    latch.countDown();
                } catch (Throwable th) {
                    logger.info("[testMultiThreadNotifyObserver]notify fail", th);
                }
            });
        });

        latch.await(5, TimeUnit.SECONDS);
        waitConditionUntilTimeOut(() -> singleThreadExecutors.getCompletedTaskCount() == multi, 10000, 100);
        verify(redisClient).sendMessage(Mockito.any(ByteBuf.class));
        verify(observer).release();
    }

}
