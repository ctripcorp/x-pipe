package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.cluster.Hints;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author ayq
 * <p>
 * 2022/8/26 16:32
 */
public class DefaultRedisGtidCollectorTest {

    private DcMetaCache dcMetaCache;

    private DefaultRedisGtidCollector defaultRedisGtidCollector;

    @Before
    public void setUp() throws Exception {
        dcMetaCache = Mockito.mock(DcMetaCache.class);
        defaultRedisGtidCollector = new DefaultRedisGtidCollector(1L,1L, dcMetaCache,
                Mockito.mock(CurrentMetaManager.class), Mockito.mock(MultiDcService.class),
                Mockito.mock(ScheduledExecutorService.class), Mockito.mock(XpipeNettyClientKeyedObjectPool.class),
                DefaultRedisGtidCollector.DEFAULT_INTERVAL_SECONDS);
    }

    @Test
    public void testSrcChanged() {
        assertFalse(defaultRedisGtidCollector.sidsChanged("", null));
        assertFalse(defaultRedisGtidCollector.sidsChanged("", "a,b"));
        assertFalse(defaultRedisGtidCollector.sidsChanged("a,b", "a,b"));
        assertFalse(defaultRedisGtidCollector.sidsChanged("a,b", "b,a"));

        assertTrue(defaultRedisGtidCollector.sidsChanged("a,b", ""));
        assertTrue(defaultRedisGtidCollector.sidsChanged("a,b,c", "b,a"));
        assertTrue(defaultRedisGtidCollector.sidsChanged("a,b", "a,b,c"));
    }

    @Test
    public void testHintsMasterDcInClusterSourceShard() {
        ClusterMeta clusterMeta = new ClusterMeta();
        clusterMeta.setHints(Hints.MASTER_DC_IN_CLUSTER.name());
        when(dcMetaCache.getClusterMeta(1L)).thenReturn(clusterMeta);
        when(dcMetaCache.isCurrentShardParentCluster(1L, 1L)).thenReturn(false);

        DefaultRedisGtidCollector collector = spy(defaultRedisGtidCollector);

        collector.work();
        verify(collector,Mockito.times(0)).collectCurrentDcGtidAndSids();
        verify(collector,Mockito.times(1)).collectSids();
    }

    @Test
    public void testHintsMasterDcInClusterNormalShard() {
        ClusterMeta clusterMeta = new ClusterMeta();
        clusterMeta.setHints(Hints.MASTER_DC_IN_CLUSTER.name());
        when(dcMetaCache.getClusterMeta(1L)).thenReturn(clusterMeta);
        when(dcMetaCache.isCurrentShardParentCluster(1L, 1L)).thenReturn(true);

        DefaultRedisGtidCollector collector = spy(defaultRedisGtidCollector);

        collector.work();
        verify(collector,Mockito.times(1)).collectCurrentDcGtidAndSids();
        verify(collector,Mockito.times(0)).collectSids();
    }

    @Test
    public void testHintsMasterDcNotInCluster() {
        ClusterMeta clusterMeta = new ClusterMeta();
        when(dcMetaCache.getClusterMeta(1L)).thenReturn(clusterMeta);

        DefaultRedisGtidCollector collector = spy(defaultRedisGtidCollector);

        collector.work();
        verify(collector,Mockito.times(0)).collectCurrentDcGtidAndSids();
        verify(collector,Mockito.times(0)).collectSids();
    }

}