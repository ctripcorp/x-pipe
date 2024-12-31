package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.SERVER_TYPE;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackup;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
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

    @Mock
    private MasterStats masterStats;

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
        when(keeperMonitor.getMasterStats()).thenReturn(masterStats);
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(replicationStoreStats);
    }

    @Test
    public void testOnMasterConnected_resetToken() {
        LeakyBucket leakyBucket = new DefaultLeakyBucket(1);
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(masterStats.lastMasterType()).thenReturn(SERVER_TYPE.REDIS);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);

        listener.onMasterConnected();
        Assert.assertTrue(listener.canSendPsync());
        Assert.assertEquals(0, leakyBucket.references());
        listener.onMasterConnected();
        Assert.assertEquals(1, leakyBucket.references());
        Assert.assertTrue(listener.canSendPsync());
        Assert.assertEquals(0, leakyBucket.references());
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
    public void testCanSendPsync() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(masterStats.lastMasterType()).thenReturn(SERVER_TYPE.REDIS);
        when(resourceManager.getLeakyBucket()).thenReturn(new DefaultLeakyBucket(1));
        Assert.assertTrue(listener.canSendPsync());
        verify(resourceManager, times(1)).getLeakyBucket();
    }

    @Test
    public void testCanSendPsyncWithFirstTimeFail() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(masterStats.lastMasterType()).thenReturn(SERVER_TYPE.REDIS);

        LeakyBucket leakyBucket = new DefaultLeakyBucket(1);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        leakyBucket.tryAcquire();
        Assert.assertFalse(listener.canSendPsync());
        Assert.assertFalse(listener.canSendPsync());
        verify(resourceManager, times(2)).getLeakyBucket();
    }

    @Test
    public void testDynamicAdjustTokenSize() {
        //increase size
        when(masterStats.lastMasterType()).thenReturn(SERVER_TYPE.REDIS);
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
        when(masterStats.lastMasterType()).thenReturn(SERVER_TYPE.REDIS);
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
    public void testConcurrentIncreaseTokenSize() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(keeperConfig.getLeakyBucketInitSize()).thenReturn(3);
        when(masterStats.lastMasterType()).thenReturn(SERVER_TYPE.REDIS);
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
        when(masterStats.lastMasterType()).thenReturn(SERVER_TYPE.REDIS);

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
        leakyBucket.doCheckKeeperConfigChange();
        int INFINITY = 20;
        for(int i = 0; i < INFINITY; i ++) {
            Assert.assertTrue(listener.canSendPsync());
        }
    }

    @Test
    public void testOnMasterDisconnected() {
        listener = spy(listener);
        listener.onMasterDisconnected();
        verify(leakyBucket, never()).release();
        listener.holdToken.set(true);
        listener.onMasterDisconnected();
        verify(leakyBucket, times(1)).release();
    }

    @Test
    public void testEndWriteRdb() {
        listener = spy(listener);
        listener.endWriteRdb();
        verify(leakyBucket, never()).release();
        listener.holdToken.set(true);
        listener.endWriteRdb();
        verify(leakyBucket, times(1)).release();
    }

    @Test
    public void testWhenDeployRateLimitWillNotBlock() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        logger.info("[{}]", redisKeeperServer.getKeeperConfig().getReplDownSafeIntervalMilli());
        Assert.assertTrue(listener.canSendPsync());
        verify(leakyBucket, never()).tryAcquire();
    }

    @Test
    public void testWhenRestart() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        replicationStoreStats.setMasterState(MASTER_STATE.REDIS_REPL_CONNECT);
        Assert.assertTrue(listener.canSendPsync());
        verify(leakyBucket, times(0)).tryAcquire();
    }

    @Test
    public void testWhenNetworkBlockTooLong() {
        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        when(redisMaster.isKeeper()).thenReturn(true);
        when(resourceManager.getLeakyBucket()).thenReturn(leakyBucket);
        when(leakyBucket.tryAcquire()).thenReturn(false);
        logger.info("[{}]", redisKeeperServer.getKeeperConfig().getReplDownSafeIntervalMilli());

        ((DefaultReplicationStoreStats)replicationStoreStats).setLastReplDownTime(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10));
        Assert.assertFalse(listener.canSendPsync());
        verify(leakyBucket, times(1)).tryAcquire();
    }
}