package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.*;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultReplicationStoreStats;
import com.ctrip.xpipe.utils.DefaultLeakyBucket;
import com.ctrip.xpipe.utils.LeakyBucket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication.KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Mar 01, 2020
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisMasterReplicationTrafficRateLimitTest extends AbstractRedisKeeperTest {
    @Mock
    private RedisMaster redisMaster;

    @Mock
    private RedisKeeperServer redisKeeperServer;

    @Mock
    private ReplicationStore replicationStore;

    @Mock
    private MetaStore metaStore;

    @Mock
    private KeeperResourceManager keeperResourceManager;

    @Mock
    private LeakyBucket leakyBucket;

    @Mock
    private KeeperMonitor keeperMonitor;

    @Spy
    private TestKeeperConfig keeperConfig = new TestKeeperConfig();

    private ReplicationStoreStats replicationStoreStats = new DefaultReplicationStoreStats();

    private NioEventLoopGroup nioEventLoopGroup;

    private KeeperStats keeperStats;

    private FakeRedisServer server;

    private AbstractRedisMasterReplication armr;

    @Before
    public void beforeRedisMasterReplicationTrafficRateLimitTest() throws Exception {
        System.setProperty(KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "1");
        nioEventLoopGroup = new NioEventLoopGroup(2);
        keeperConfig.setReplDownSafeIntervalMilli(5*1000*60L);
        when(redisKeeperServer.getKeeperConfig()).thenReturn(keeperConfig);
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(redisKeeperServer.getKeeperMonitor()).thenReturn(keeperMonitor);
        keeperStats = spy(new DefaultKeeperStats("shard", scheduled));
        when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(replicationStoreStats);
        when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
        when(replicationStore.getMetaStore()).thenReturn(metaStore);
        server = startFakeRedisServer();
        when(redisMaster.masterEndPoint()).thenReturn(new DefaultProxyEndpoint("127.0.0.1", server.getPort()));
        armr = new ControllableRedisMasterReplication(redisKeeperServer, redisMaster,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
    }

    @After
    public void afterRedisMasterReplicationTrafficRateLimitTest() throws Exception {
        nioEventLoopGroup.shutdown();
        if (server != null) {
            server.stop();
        }
    }

    // first time try psync will failure due to a lack of token
    // but second time should work
    @Test
    public void testFullSyncWaitingForToken() throws Exception {
        doNothing().when(leakyBucket).release();
        when(leakyBucket.tryAcquire()).thenReturn(false).thenReturn(true);
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);
        waitConditionUntilTimeOut(()->keeperStats.getFullSyncCount() >= 1, 6000);
        Assert.assertTrue(2 <= ((ControllableRedisMasterReplication)armr).getConnectTimes());
    }

    // test full sync failed after retrieve a token will not bother another psync which is waiting for token
    @Test
    public void testFullSyncFailHalfWayWontBother() throws Exception {
        server.setSendHalfRdbAndCloseConnectionCount(1);
        LeakyBucket leakyBucket = new DefaultLeakyBucket(1);
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        CountDownLatch countDownLatch = new CountDownLatch(2);
        ControllableRedisMasterReplication otherReplication = new ControllableRedisMasterReplication(redisKeeperServer, redisMaster,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    LifecycleHelper.initializeIfPossible(armr);
                    LifecycleHelper.startIfPossible(armr);
                } catch (Exception ignore) {
                } finally {
                    countDownLatch.countDown();
                }
            }
        });
        executors.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sleep(30);
                    LifecycleHelper.initializeIfPossible(otherReplication);
                    LifecycleHelper.startIfPossible(otherReplication);
                } catch (Exception ignore) {
                } finally {
                    countDownLatch.countDown();
                }
            }
        });
        countDownLatch.await(500, TimeUnit.MILLISECONDS);
        waitConditionUntilTimeOut(()->keeperStats.getFullSyncCount() == 3, 5000);
        Assert.assertTrue(2 <= ((ControllableRedisMasterReplication)armr).getConnectTimes());
        Assert.assertTrue(1 <= otherReplication.getConnectTimes());
    }

    private void makePartialSync() {
        // make it partial sync
        if(armr instanceof ControllableRedisMasterReplication) {
            ((ControllableRedisMasterReplication) armr).setOffset(server.getRdbOffset() + 1).setReplId(server.getRunId());
        } else if(armr instanceof InMemoryDefaultReplication) {
            ((InMemoryDefaultReplication) armr).setOffset(server.getRdbOffset() + 1).setReplId(server.getRunId());
        }
    }
    // test partial sync will finally release token after sort of time
    @Test
    public void testPartialSyncReleaseTokenEventually() throws Exception {
        makePartialSync();
        LeakyBucket leakyBucket = new DefaultLeakyBucket(1);
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        when(redisKeeperServer.getKeeperConfig())
                .thenReturn(new TestKeeperConfig(1000, 2, 1000, 1000)
                        .setReplHighWaterMark(1000).setReplLowWaterMark(20).setMaxPartialSyncKeepTokenRounds(2).setPartialSyncTrafficMonitorIntervalTimes(1));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);

        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 5000);
        // sleep 4 round to let partial sync return the token
        sleep(4 * 100);
        Assert.assertEquals(1, ((ControllableRedisMasterReplication)armr).getConnectTimes());
        Assert.assertEquals(1, leakyBucket.references());
    }

    // test partial sync release a token regardless a every limit speed
    @Test
    public void testPartialSyncReleaseTokenASAP() throws Exception {
        makePartialSync();
        LeakyBucket leakyBucket = new DefaultLeakyBucket(1);
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        when(redisKeeperServer.getKeeperConfig())
                .thenReturn(new TestKeeperConfig(1000, 2, 1000, 1000)
                        .setReplHighWaterMark(1000).setReplLowWaterMark(1000).setMaxPartialSyncKeepTokenRounds(10).setPartialSyncTrafficMonitorIntervalTimes(1));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);

        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 5000);
        // sleep 4 round to let partial sync return the token
        sleep(4 * 100);
        Assert.assertEquals(1, ((ControllableRedisMasterReplication)armr).getConnectTimes());
        Assert.assertEquals(1, leakyBucket.references());
    }

    // as it says
    @Test
    public void testRateLimitWontImpactBackupKeeper() throws Exception {
        makePartialSync();
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateBackup(redisKeeperServer));
        LeakyBucket leakyBucket = spy(new DefaultLeakyBucket(1));
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 2000);
        verify(leakyBucket, never()).tryAcquire();
    }

    @Test
    public void testRateLimitWontInfluencePrimaryDcKeeper() throws Exception {
        makePartialSync();
        when(redisMaster.isKeeper()).thenReturn(false);
        LeakyBucket leakyBucket = spy(new DefaultLeakyBucket(1));
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 2000);
        verify(leakyBucket, never()).tryAcquire();
    }

    @Test
    public void testTokenWillBeFreedDueToFullSyncFailure() throws Exception {
        server.setSendHalfRdbAndCloseConnectionCount(1);
        LeakyBucket leakyBucket = spy(new DefaultLeakyBucket(1));
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getFullSyncCount() == 2, 2000);
        sleep(100);
        verify(leakyBucket, times(1)).release();
    }

    @Test
    public void testTokenShallReleaseAfterPartialSyncFailure() throws Exception {
        makePartialSync();
        LeakyBucket leakyBucket = new DefaultLeakyBucket(1);
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        when(redisKeeperServer.getKeeperConfig())
                .thenReturn(new TestKeeperConfig(100, 2, 1000, 1000).setReplHighWaterMark(1000).setReplLowWaterMark(20));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);

        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 5000);
        //now disturb the link when partial sync
        server.stop();
        server = null;
        armr.stop();
        sleep(100);
        Assert.assertEquals(1, leakyBucket.references());
    }

    @Test
    public void testDeployOrSwapWontRateLimit() throws Exception {
        when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(replicationStoreStats);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                keeperMonitor.getKeeperStats().increaseFullSync();
                return null;
            }
        }).when(redisKeeperServer).onFullSync();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                keeperMonitor.getKeeperStats().increatePartialSync();
                return null;
            }
        }).when(redisKeeperServer).onContinue(anyString(), anyString());
        RedisMaster redisMaster = spy(new DefaultRedisMaster(redisKeeperServer,
                new DefaultEndPoint("127.0.0.1", server.getPort()), nioEventLoopGroup, createReplicationStoreManager(),
                scheduled, keeperResourceManager));
        when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
        when(redisKeeperServer.getRedisMaster()).thenReturn(redisMaster);
        //first, be backup keeper, then assume the active keeper is restarting, it's turn to active
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateBackup(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        armr = new InMemoryDefaultReplication(redisMaster, redisKeeperServer,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        makePartialSync();
        LeakyBucket leakyBucket = spy(new DefaultLeakyBucket(1));
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        when(redisKeeperServer.getKeeperConfig())
                .thenReturn(new TestKeeperConfig(100, 2, 1000, 1000)
                        .setReplHighWaterMark(1000).setReplLowWaterMark(20).setReplDownSafeIntervalMilli(5*1000*60L));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 5000);
        //now disturb the link when partial sync
        server.stop();
        LifecycleHelper.stopIfPossible(armr);
        LifecycleHelper.disposeIfPossible(armr);

        sleep(200);
        Assert.assertEquals(1, leakyBucket.references());
        Assert.assertNotEquals(0, replicationStoreStats.getReplDownSince());
        verify(leakyBucket, never()).tryAcquire();

        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);

        armr = new InMemoryDefaultReplication(redisMaster, redisKeeperServer,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        server.start();
        makePartialSync();
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);
        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 2, 5000);
        Assert.assertEquals(0, replicationStoreStats.getReplDownSince());
        // leaky bucket will not need when second time
        verify(leakyBucket, never()).tryAcquire();
    }

    @Test
    public void testRestartWillRateLimit() throws Exception {
        when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(replicationStoreStats);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                keeperMonitor.getKeeperStats().increaseFullSync();
                return null;
            }
        }).when(redisKeeperServer).onFullSync();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                keeperMonitor.getKeeperStats().increatePartialSync();
                return null;
            }
        }).when(redisKeeperServer).onContinue(anyString(), anyString());
        RedisMaster redisMaster = spy(new DefaultRedisMaster(redisKeeperServer,
                new DefaultEndPoint("127.0.0.1", server.getPort()), nioEventLoopGroup, createReplicationStoreManager(),
                scheduled, keeperResourceManager));
        when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
        when(redisKeeperServer.getRedisMaster()).thenReturn(redisMaster);
        //first, be backup keeper, then assume the active keeper is restarting, it's turn to active
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);

        makePartialSync();
        LeakyBucket leakyBucket = spy(new DefaultLeakyBucket(1));
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        when(redisKeeperServer.getKeeperConfig())
                .thenReturn(new TestKeeperConfig(100, 2, 1000, 1000)
                        .setReplHighWaterMark(1000).setReplLowWaterMark(20).setReplDownSafeIntervalMilli(5*1000*60L));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 5000);
        //now disturb the link when partial sync
        server.stop();
        LifecycleHelper.stopIfPossible(armr);
        LifecycleHelper.disposeIfPossible(armr);

        sleep(100);
        Assert.assertEquals(1, leakyBucket.references());
        Assert.assertNotEquals(0, replicationStoreStats.getReplDownSince());
        verify(leakyBucket, times(1)).tryAcquire();

        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);

        server.start();
        //new replication, as restart will cause a new one
        // also new stats, as restart will cause a new one
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(new DefaultReplicationStoreStats());
        armr = new InMemoryDefaultReplication(redisMaster, redisKeeperServer,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        makePartialSync();
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);
        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() > 1, 2000);
        waitConditionUntilTimeOut(()->leakyBucket.references() >= 1, 2000);
        verify(leakyBucket, times(2)).tryAcquire();
        Assert.assertEquals(0, keeperMonitor.getReplicationStoreStats().getReplDownSince());

    }

    @Test
    public void testNotSuccessfulConnectWithMasterShouldNotRefreshReplDown() throws Exception {
        when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(replicationStoreStats);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                keeperMonitor.getKeeperStats().increaseFullSync();
                return null;
            }
        }).when(redisKeeperServer).onFullSync();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                keeperMonitor.getKeeperStats().increatePartialSync();
                return null;
            }
        }).when(redisKeeperServer).onContinue(anyString(), anyString());
        RedisMaster redisMaster = spy(new DefaultRedisMaster(redisKeeperServer,
                new DefaultEndPoint("127.0.0.1", server.getPort()), nioEventLoopGroup, createReplicationStoreManager(),
                scheduled, keeperResourceManager));
        when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
        when(redisKeeperServer.getRedisMaster()).thenReturn(redisMaster);

        int THREE_TIMES_FULL_SYNC_FAILURE = 3;
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);

        LeakyBucket leakyBucket = spy(new DefaultLeakyBucket(3));
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        TestKeeperConfig keeperConfig = new TestKeeperConfig(100, 2, 1000, 1000)
                .setReplHighWaterMark(1000).setReplLowWaterMark(20).setReplDownSafeIntervalMilli(1000);
        when(redisKeeperServer.getKeeperConfig())
                .thenReturn(keeperConfig);
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);

        server.setSendHalfRdbAndCloseConnectionCount(THREE_TIMES_FULL_SYNC_FAILURE);

        armr = new InMemoryDefaultReplication(redisMaster, redisKeeperServer,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        makePartialSync();
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);
        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() >= 1, 2000);
        verify(leakyBucket, times(1)).tryAcquire();
        // this will refresh the repl_down_since
        LifecycleHelper.stopIfPossible(armr);
        LifecycleHelper.disposeIfPossible(armr);

        sleep(500); // not more than 1000, safe to let it not go through rate limit
        armr = new InMemoryDefaultReplication(redisMaster, redisKeeperServer,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);
        waitConditionUntilTimeOut(()->keeperStats.getFullSyncCount() >= 1, 2000);
        // should not borrow token
        verify(leakyBucket, times(1)).tryAcquire();
        // this should not refresh the repl_down_since !!!!!
        LifecycleHelper.stopIfPossible(armr);
        LifecycleHelper.disposeIfPossible(armr);

        sleep(500);
        armr = new InMemoryDefaultReplication(redisMaster, redisKeeperServer,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);
        waitConditionUntilTimeOut(()->keeperStats.getFullSyncCount() >= 2, 2000);
        // this time, we are over due, should tryAcquire
        verify(leakyBucket, times(2)).tryAcquire();

        LifecycleHelper.stopIfPossible(armr);
        LifecycleHelper.disposeIfPossible(armr);

    }

    @Test
    public void testBugReplicationStartNewBeforeCloseOldNettyChannel() throws Exception {
        when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(replicationStoreStats);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                keeperMonitor.getKeeperStats().increaseFullSync();
                return null;
            }
        }).when(redisKeeperServer).onFullSync();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                keeperMonitor.getKeeperStats().increatePartialSync();
                return null;
            }
        }).when(redisKeeperServer).onContinue(anyString(), anyString());
        RedisMaster redisMaster = spy(new DefaultRedisMaster(redisKeeperServer,
                new DefaultEndPoint("127.0.0.1", server.getPort()), nioEventLoopGroup, createReplicationStoreManager(),
                scheduled, keeperResourceManager));
        when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
        when(redisKeeperServer.getRedisMaster()).thenReturn(redisMaster);

        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateBackup(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);

        LeakyBucket leakyBucket = spy(new DefaultLeakyBucket(3));
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        TestKeeperConfig keeperConfig = new TestKeeperConfig(100, 2, 1000, 1000)
                .setReplHighWaterMark(1000).setReplLowWaterMark(20).setReplDownSafeIntervalMilli(1000);
        when(redisKeeperServer.getKeeperConfig())
                .thenReturn(keeperConfig);
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);

        armr = new InMemoryDefaultReplication(redisMaster, redisKeeperServer,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);
        waitConditionUntilTimeOut(()->keeperStats.getFullSyncCount() >= 1, 2000);
        // should not borrow token
        verify(leakyBucket, never()).tryAcquire();
        waitConditionUntilTimeOut(()->redisMaster.getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED, 2000);
        // this should not refresh the repl_down_since !!!!!
        LifecycleHelper.stopIfPossible(armr);
        LifecycleHelper.disposeIfPossible(armr);

        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        armr = new InMemoryDefaultReplication(redisMaster, redisKeeperServer,
                nioEventLoopGroup, scheduled, keeperResourceManager);
        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);
        waitConditionUntilTimeOut(()->keeperStats.getFullSyncCount() >= 2, 2000);
        // here's the bug
        // stop/dispose will call "channel.close()", but netty will trigger the real 'channelClosed' event
        // only in eventloop, that's unexpected time
        // we try to simulate the bug, as we believe normally we'll start before netty calls
        verify(leakyBucket, never()).tryAcquire();

        LifecycleHelper.stopIfPossible(armr);
        LifecycleHelper.disposeIfPossible(armr);
    }

    private static class InMemoryDefaultReplication extends DefaultRedisMasterReplication {

        private String replId;

        private int offset;

        public InMemoryDefaultReplication setReplId(String replId) {
            this.replId = replId;
            return this;
        }

        public InMemoryDefaultReplication setOffset(int offset) {
            this.offset = offset;
            return this;
        }

        public InMemoryDefaultReplication(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer, NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled, int replTimeoutMilli, KeeperResourceManager resourceManager) {
            super(redisMaster, redisKeeperServer, nioEventLoopGroup, scheduled, replTimeoutMilli, resourceManager);
        }

        public InMemoryDefaultReplication(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer, NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled, KeeperResourceManager resourceManager) {
            super(redisMaster, redisKeeperServer, nioEventLoopGroup, scheduled, resourceManager);
        }

        @Override
        protected Psync createPsync() {
            Psync psync = new InMemoryPsync(clientPool, replId, offset, scheduled);
            psync.addPsyncObserver(this);
            psync.addPsyncObserver(super.redisKeeperServer);
            return psync;
        }
    }

    private static class ControllableRedisMasterReplication extends AbstractRedisMasterReplication {

        private static final AtomicInteger counter = new AtomicInteger(0);

        private String replId;

        private int offset;

        public ControllableRedisMasterReplication setReplId(String replId) {
            this.replId = replId;
            return this;
        }

        public ControllableRedisMasterReplication setOffset(int offset) {
            this.offset = offset;
            return this;
        }

        public volatile int connectTimes;

        public ControllableRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster,
                                                  NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
                                                  int replTimeoutMilli, KeeperResourceManager resourceManager) {
            super(redisKeeperServer, redisMaster, nioEventLoopGroup, scheduled, replTimeoutMilli, resourceManager);
            counter.incrementAndGet();
        }

        public ControllableRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster,
                                                  NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
                                                  KeeperResourceManager resourceManager) {
            super(redisKeeperServer, redisMaster, nioEventLoopGroup, scheduled, resourceManager);
            counter.incrementAndGet();
        }

        public int getConnectTimes() {
            return connectTimes;
        }

        @Override
        protected String getSimpleName() {
            return getClass().getSimpleName() + counter.get();
        }

        @Override
        protected void doConnect(Bootstrap b) {
            connectTimes ++;
            redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTING);

            tryConnect(b).addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future) throws Exception {

                    if(!future.isSuccess()){

                        logger.error("[operationComplete][fail connect with master]" + redisMaster, future.cause());

                        scheduled.schedule(new Runnable() {
                            @Override
                            public void run() {
                                try{
                                    connectWithMaster();
                                }catch(Throwable th){
                                    logger.error("[run][connectUntilConnected]" + ControllableRedisMasterReplication.this, th);
                                }
                            }
                        }, masterConnectRetryDelaySeconds, TimeUnit.SECONDS);
                    }
                }
            });
        }

        @Override
        public void masterDisconntected(Channel channel) {
            super.masterDisconntected(channel);

            redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().refreshReplDownSince(System.currentTimeMillis());
            long interval = System.currentTimeMillis() - connectedTime;
            long scheduleTime = masterConnectRetryDelaySeconds * 1000 - interval;
            if (scheduleTime < 0) {
                scheduleTime = 0;
            }
            logger.info("[masterDisconntected][reconnect after {} ms]", scheduleTime);
            scheduled.schedule(new AbstractExceptionLogTask() {

                @Override
                public void doRun() {
                    connectWithMaster();
                }
            }, scheduleTime, TimeUnit.MILLISECONDS);
        }

        @Override
        protected Psync createPsync() {
            Psync psync = new InMemoryPsync(clientPool, replId, offset, scheduled);
            psync.addPsyncObserver(this);
            return psync;
        }

        @Override
        protected void psyncFail(Throwable cause) {
            redisKeeperServer.getKeeperMonitor().getKeeperStats().increatePartialSyncError();
        }

        @Override
        protected void doOnFullSync() {
            setRdbDumper(new RedisMasterReplicationRdbDumper(this, redisKeeperServer, resourceManager));
            redisKeeperServer.getKeeperMonitor().getKeeperStats().increaseFullSync();
        }

        @Override
        protected void doReFullSync() {

        }

        @Override
        protected void doBeginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {

        }

        @Override
        protected void doEndWriteRdb() {
            redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().refreshReplDownSince(0);
        }

        @Override
        protected void doOnContinue() {
            redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().refreshReplDownSince(0);
            redisKeeperServer.getKeeperMonitor().getKeeperStats().increatePartialSync();
        }

        @Override
        public PARTIAL_STATE partialState() {
            return null;
        }
    }
}
