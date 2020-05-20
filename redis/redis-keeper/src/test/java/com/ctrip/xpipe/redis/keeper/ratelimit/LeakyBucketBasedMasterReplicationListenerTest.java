package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackup;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultReplicationStoreStats;
import com.ctrip.xpipe.utils.DefaultLeakyBucket;
import com.ctrip.xpipe.utils.LeakyBucket;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Mar 06, 2020
 */
public class LeakyBucketBasedMasterReplicationListenerTest extends AbstractTest {

    @Mock
    private RedisMasterReplication redisMasterReplication;

    @Mock
    private KeeperResourceManager resourceManager;

    @Mock
    private RedisKeeperServer redisKeeperServer;

    @Mock
    private LeakyBucket leakyBucket;

    @Mock
    private RedisMaster redisMaster;

    @Mock
    private KeeperMonitor keeperMonitor;

    @Mock
    private MetaServerKeeperService metaServerKeeperService;

    @Mock
    private KeeperContainerService keeperContainerService;

    @Spy
    private KeeperConfig keeperConfig = new DefaultKeeperConfig();

    private ReplicationStoreStats replicationStoreStats = new DefaultReplicationStoreStats();

    private KeeperStats keeperStats;

    private LeakyBucketBasedMasterReplicationListener listener;

    @Before
    public void beforeLeakyBucketBasedMasterReplicationListenerTest() {
        MockitoAnnotations.initMocks(this);
        listener = new LeakyBucketBasedMasterReplicationListener(redisMasterReplication, redisKeeperServer,
                resourceManager, scheduled);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(redisKeeperServer.getRedisMaster()).thenReturn(redisMaster);
        when(redisMasterReplication.redisMaster()).thenReturn(redisMaster);
        when(redisKeeperServer.getKeeperMonitor()).thenReturn(keeperMonitor);
        when(redisKeeperServer.getKeeperConfig()).thenReturn(keeperConfig);
        keeperStats = spy(new DefaultKeeperStats("shard", scheduled));
        when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(replicationStoreStats);
    }

    @Test
    public void testOnMasterConnected() {
    }

    @Test
    public void testCanSendPsyncWithNonActiveState() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateBackup(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        verify(resourceManager, never()).getLeakyBucket();
        Assert.assertTrue(listener.canSendPsync());
    }
    @Test
    public void testCanSendPsyncWithMasterNotKeeper() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(false);
        verify(resourceManager, never()).getLeakyBucket();
        Assert.assertTrue(listener.canSendPsync());
    }
    @Test
    public void testCanSendPsyncWithAlwaysFail() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        LeakyBucket leakyBucket = spy(new DefaultLeakyBucket(1));
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        Assert.assertTrue(listener.canSendPsync());
        listener.onDumpFail();
        Assert.assertTrue(listener.canSendPsync());
        listener.onDumpFail();
        Assert.assertTrue(listener.canSendPsync());
        verify(leakyBucket, times(1)).tryAcquire();
    }
    @Test
    public void testCanSendPsync() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(resourceManager.getLeakyBucket()).thenReturn(new DefaultLeakyBucket(1));
        Assert.assertTrue(listener.canSendPsync());
        verify(resourceManager, times(1)).getLeakyBucket();
    }

    @Test
    public void testCanSendPsyncWithFirstTimeFail() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        LeakyBucket leakyBucket = new DefaultLeakyBucket(1);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        leakyBucket.tryAcquire();
        Assert.assertFalse(listener.canSendPsync());
        Assert.assertFalse(listener.canSendPsync());
        verify(resourceManager, times(2)).getLeakyBucket();
    }

    @Test
    public void testOverload() {
        logger.info("{}", 1000L * 6 * 1073741824 / 104857600L);
    }

    @Test
    public void testIsTokenReadyToReleaseWhenDeadline() {
        long deadline = System.currentTimeMillis() - 10;
        Assert.assertTrue(listener.isTokenReadyToRelease(deadline));
        deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
    }

    @Test
    public void testIsTokenReadyToReleaseWhenPeakBPSHuge() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        when(keeperConfig.getReplicationTrafficLowWaterMark()).thenReturn(1024 * 1024 * 20L); // 20MB/s
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 21L); //21MB/s
        when(keeperStats.getPeakInputInstantaneousBPS()).thenReturn(1024 * 1024 * 50L); //50MB/s
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertTrue(listener.isTokenReadyToRelease(deadline));
    }

    @Test
    public void testIsTokenReadyToReleaseWhenPeakBPSTiny() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        when(keeperConfig.getReplicationTrafficLowWaterMark()).thenReturn(1024 * 1024 * 20L); // 20MB/s
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 21L); //21MB/s
        when(keeperStats.getPeakInputInstantaneousBPS()).thenReturn(1024 * 1024 * 30L); //30MB/s
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
    }

    @Test
    public void testIsTokenReadyToReleaseWhenPeakBPSChange() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        when(keeperConfig.getReplicationTrafficLowWaterMark()).thenReturn(1024 * 1024 * 20L); // 20MB/s
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 21L); //21MB/s
        when(keeperStats.getPeakInputInstantaneousBPS()).thenReturn(1024 * 1024 * 50L); //50MB/s
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        when(keeperStats.getPeakInputInstantaneousBPS()).thenReturn(1024 * 1024 * 30L); //30MB/s
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        when(keeperStats.getPeakInputInstantaneousBPS()).thenReturn(1024 * 1024 * 50L); //50MB/s
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertTrue(listener.isTokenReadyToRelease(deadline));
    }

    @Test
    public void testIsTokenReadyToReleaseWhenLessThan20MB() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 2L); //2MB/s
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertTrue(listener.isTokenReadyToRelease(deadline));
    }

    @Test
    public void testIsTokenReadyToReleaseIfNotContinuously() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 2L); //2MB/s
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 21L);
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 2L);
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
        Assert.assertTrue(listener.isTokenReadyToRelease(deadline));
    }

    @Test
    public void testLeakyBucketReleaseShouldAlsoResetCounter() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(resourceManager.getLeakyBucket()).thenReturn(new DefaultLeakyBucket(1));
        Assert.assertTrue(listener.canSendPsync());
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 2L);
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(100);
        listener.checkIfNeedReleaseToken(deadline, 10);
        sleep(100 * 3 + 15);
        // after sleep, token is released, now we shall reset all
        // make sure, we reset the counter, so that we cannot pass in the first time(check 3 times)
        Assert.assertFalse(listener.isTokenReadyToRelease(System.currentTimeMillis() + 1100));
    }

    @Test
    public void testDynamicAdjustTokenSize() {
        //increase size
        when(keeperConfig.getLeakyBucketInitSize()).thenReturn(3);
        CompositeLeakyBucket leakyBucket = new CompositeLeakyBucket(keeperConfig, metaServerKeeperService, keeperContainerService);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        listener = new LeakyBucketBasedMasterReplicationListener(redisMasterReplication, redisKeeperServer,
                resourceManager, scheduled);
        // in case leakyBucket.refresh() not run
        when(keeperConfig.getMetaServerAddress()).thenReturn("http://xpipe.meta.com");
        MetaServerKeeperService.KeeperContainerTokenStatusResponse response = new MetaServerKeeperService
                .KeeperContainerTokenStatusResponse(keeperConfig.getLeakyBucketInitSize() + 1);
        when(metaServerKeeperService.refreshKeeperContainerTokenStatus(any())).thenReturn(response);
        leakyBucket.refresh();
        Assert.assertEquals(keeperConfig.getLeakyBucketInitSize() + 1, leakyBucket.getTotalSize());
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        for(int i = 0; i < keeperConfig.getLeakyBucketInitSize() + 1; i++) {
            Assert.assertTrue(listener.canSendPsync());
        }
        Assert.assertFalse(listener.canSendPsync());
    }

    @Test
    public void testDynamicAdjustTokenSize2() {
        //decrease size
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(keeperConfig.getLeakyBucketInitSize()).thenReturn(3);
        CompositeLeakyBucket leakyBucket = new CompositeLeakyBucket(keeperConfig, metaServerKeeperService, keeperContainerService);
        leakyBucket.setScheduled(scheduled);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        listener = new LeakyBucketBasedMasterReplicationListener(redisMasterReplication, redisKeeperServer,
                resourceManager, scheduled);
        for(int i = 0; i < keeperConfig.getLeakyBucketInitSize(); i++) {
            Assert.assertTrue(listener.canSendPsync());
        }
        Assert.assertFalse(listener.canSendPsync());
        // in case leakyBucket.refresh() not run
        when(keeperConfig.getMetaServerAddress()).thenReturn("http://xpipe.meta.com");
        MetaServerKeeperService.KeeperContainerTokenStatusResponse response = new MetaServerKeeperService
                .KeeperContainerTokenStatusResponse(keeperConfig.getLeakyBucketInitSize());
        when(metaServerKeeperService.refreshKeeperContainerTokenStatus(any())).thenReturn(response);
        int originSize = keeperConfig.getLeakyBucketInitSize();
        when(keeperConfig.getLeakyBucketInitSize()).thenReturn(originSize - 2);
        leakyBucket.checkKeeperConfigChange();
        sleep(110);
        for(int i = 0; i < originSize; i++) {
            leakyBucket.release();
        }
        Assert.assertEquals(originSize -2 , leakyBucket.getTotalSize());
        for(int i = 0; i < originSize - 2; i++) {
            Assert.assertTrue(listener.canSendPsync());
        }
        Assert.assertFalse(listener.canSendPsync());
    }

    @Test
    public void testConcurrentDecreaseTokenSize() {

    }

    @Test
    public void testConcurrentIncreaseTokenSize() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(keeperConfig.getLeakyBucketInitSize()).thenReturn(3);
        CompositeLeakyBucket leakyBucket = new CompositeLeakyBucket(keeperConfig, metaServerKeeperService, keeperContainerService);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        listener = new LeakyBucketBasedMasterReplicationListener(redisMasterReplication, redisKeeperServer,
                resourceManager, scheduled);
        for(int i = 0; i < keeperConfig.getLeakyBucketInitSize(); i++) {
            Assert.assertTrue(listener.canSendPsync());
        }
        Assert.assertFalse(listener.canSendPsync());
        // in case leakyBucket.refresh() not run
        when(keeperConfig.getMetaServerAddress()).thenReturn("http://xpipe.meta.com");
        MetaServerKeeperService.KeeperContainerTokenStatusResponse response = new MetaServerKeeperService
                .KeeperContainerTokenStatusResponse(keeperConfig.getLeakyBucketInitSize() + 1);
        when(metaServerKeeperService.refreshKeeperContainerTokenStatus(any())).thenReturn(response);
        leakyBucket.refresh();
        Assert.assertEquals(keeperConfig.getLeakyBucketInitSize() + 1, leakyBucket.getTotalSize());
        Assert.assertTrue(listener.canSendPsync());
        Assert.assertFalse(listener.canSendPsync());
    }

    @Test
    public void testDynamicCloseIt() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(keeperConfig.getLeakyBucketInitSize()).thenReturn(3);
        CompositeLeakyBucket leakyBucket = new CompositeLeakyBucket(keeperConfig, metaServerKeeperService, keeperContainerService);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        listener = new LeakyBucketBasedMasterReplicationListener(redisMasterReplication, redisKeeperServer,
                resourceManager, scheduled);
        for(int i = 0; i < keeperConfig.getLeakyBucketInitSize(); i++) {
            Assert.assertTrue(listener.canSendPsync());
        }
        Assert.assertFalse(listener.canSendPsync());
        // in case leakyBucket.refresh() not run
        when(keeperConfig.isKeeperRateLimitOpen()).thenReturn(false);
        leakyBucket.setScheduled(scheduled);
        leakyBucket.checkKeeperConfigChange();
        sleep(110);
        int INFINITY = 1024;
        for(int i = 0; i < INFINITY; i ++) {
            Assert.assertTrue(listener.canSendPsync());
        }
    }

    @Test
    public void testReleaseToken() {
        listener = spy(listener);
        listener.releaseToken();
        verify(leakyBucket, times(1)).release();
        int INIT_STATE = 1;
        Assert.assertEquals(INIT_STATE, listener.psyncEverSucceed.get());
    }

    @Test
    public void testOnMasterDisconnected() {
        listener = spy(listener);
        listener.onMasterDisconnected();
        verify(listener, never()).releaseToken();
        listener.holdToken.set(true);
        listener.onMasterDisconnected();
        verify(listener, times(1)).releaseToken();
    }

    @Test
    public void testEndWriteRdb() {
        listener = spy(listener);
        listener.endWriteRdb();
        verify(listener, never()).releaseToken();
        listener.holdToken.set(true);
        listener.endWriteRdb();
        verify(listener, times(1)).releaseToken();
    }

    @Test
    public void testOnContinue() {
    }

    @Test
    public void testOnFullSync() {
    }

    @Test
    public void testReFullSync() {
    }

    @Test
    public void testOnDumpFinished() {
    }

    @Test
    public void testOnDumpFail() {
        listener = spy(listener);
        listener.holdToken.set(true);
        listener.onDumpFail();
        verify(leakyBucket, times(1)).release();
        int KEEP_FAIL = 3;
        Assert.assertEquals(KEEP_FAIL, listener.psyncEverSucceed.get());
    }

    @Test
    public void testBeginWriteRdb() {
    }

    @Test
    public void testWhenDeployRateLimitWillNotBlock() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        logger.info("[{}]", redisKeeperServer.getKeeperConfig().getReplDownSafeIntervalMilli());
        replicationStoreStats.refreshReplDownSince(System.currentTimeMillis());
        Assert.assertTrue(listener.canSendPsync());
        verify(leakyBucket, never()).tryAcquire();
    }

    @Test
    public void testWhenRestart() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        replicationStoreStats.refreshReplDownSince(0);
        Assert.assertTrue(listener.canSendPsync());
        verify(leakyBucket, times(1)).tryAcquire();
    }

    @Test
    public void testWhenNetworkBlockTooLong() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(leakyBucket.tryAcquire()).thenReturn(false);
        logger.info("[{}]", redisKeeperServer.getKeeperConfig().getReplDownSafeIntervalMilli());
        replicationStoreStats.refreshReplDownSince(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10));
        Assert.assertFalse(listener.canSendPsync());
        verify(leakyBucket, times(1)).tryAcquire();
    }
}