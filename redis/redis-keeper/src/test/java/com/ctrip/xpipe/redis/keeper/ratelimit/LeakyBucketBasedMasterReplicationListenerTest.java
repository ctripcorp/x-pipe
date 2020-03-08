package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.entity.Keeper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackup;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeeperStats;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
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

    @Spy
    private KeeperConfig keeperConfig = new DefaultKeeperConfig();

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
        keeperStats = spy(new DefaultKeeperStats(scheduled));
        when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
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
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateBackup(redisKeeperServer));
        listener.onDumpFail();
        listener.onDumpFail();
        Assert.assertTrue(listener.canSendPsync());
        verify(resourceManager, never()).getLeakyBucket();
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
        deadline = System.currentTimeMillis() + 10;
        Assert.assertTrue(listener.isTokenReadyToRelease(deadline));
        deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        Assert.assertFalse(listener.isTokenReadyToRelease(deadline));
    }

    @Test
    public void testIsTokenReadyToReleaseWhenLessThan20MB() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        when(keeperStats.getInputInstantaneousBPS()).thenReturn(1024 * 1024 * 2L); //2kB/s
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
    public void testOnMasterDisconnected() {
    }

    @Test
    public void testEndWriteRdb() {
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
    }

    @Test
    public void testBeginWriteRdb() {
    }
}