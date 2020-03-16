package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
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
import com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackup;
import com.ctrip.xpipe.redis.keeper.impl.RedisMasterReplicationRdbDumper;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeeperStats;
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
import org.mockito.runners.MockitoJUnitRunner;

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

    private NioEventLoopGroup nioEventLoopGroup;

    private KeeperStats keeperStats;

    private FakeRedisServer server;

    private AbstractRedisMasterReplication armr;

    @Before
    public void beforeRedisMasterReplicationTrafficRateLimitTest() throws Exception {
        System.setProperty(KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "1");
        nioEventLoopGroup = new NioEventLoopGroup(2);
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(redisKeeperServer.getKeeperMonitor()).thenReturn(keeperMonitor);
        keeperStats = spy(new DefaultKeeperStats("shard", scheduled));
        when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
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
        ((ControllableRedisMasterReplication) armr).setOffset(server.getRdbOffset() + 1).setReplId(server.getRunId());
    }
    // test partial sync will finally release token after sort of time
    @Test
    public void testPartialSyncReleaseTokenEventually() throws Exception {
        makePartialSync();
        LeakyBucket leakyBucket = new DefaultLeakyBucket(1);
        when(keeperResourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        when(redisKeeperServer.getKeeperConfig())
                .thenReturn(new TestKeeperConfig(100, 2, 1000, 1000).setReplHighWaterMark(1000).setReplLowWaterMark(20));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);

        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 5000);
        // sleep to let partial sync return the token
        sleep(420);
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
                .thenReturn(new TestKeeperConfig(100, 2, 1000, 1000).setReplHighWaterMark(1000).setReplLowWaterMark(1000));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(100L);

        LifecycleHelper.initializeIfPossible(armr);
        LifecycleHelper.startIfPossible(armr);

        waitConditionUntilTimeOut(()->keeperStats.getPartialSyncCount() == 1, 5000);
        // sleep to let partial sync return the token
        sleep(320);
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

        }

        @Override
        protected void doOnContinue() {
            redisKeeperServer.getKeeperMonitor().getKeeperStats().increatePartialSync();
        }

        @Override
        public PARTIAL_STATE partialState() {
            return null;
        }
    }
}
