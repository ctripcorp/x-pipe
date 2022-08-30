package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;

import static org.mockito.Mockito.*;

/**
 * @author ayq
 * <p>
 * 2022/4/26 16:25
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisMetaChangeManagerTest {

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private MultiDcService multiDcService;

    private DefaultRedisMetaChangeManager redisMetaChangeManager;

    @Before
    public void beforeDefaultRedisMetaChangeManagerTest() {

        redisMetaChangeManager = new DefaultRedisMetaChangeManager();

        redisMetaChangeManager.setDcMetaCache(dcMetaCache);
        redisMetaChangeManager.setCurrentMetaManager(currentMetaManager);
        redisMetaChangeManager.setMultiDcService(multiDcService);
    }

    @Test
    public void testSidsChange() {

        ClusterMetaComparator comparator = mock(ClusterMetaComparator.class);
        when(comparator.getCurrent()).thenReturn(new ClusterMeta());
        ShardMetaComparator shardMetaComparator = mock(ShardMetaComparator.class);
        when(comparator.getMofified()).thenReturn(new HashSet<>(Arrays.asList(shardMetaComparator)));
        when(shardMetaComparator.getFuture()).thenReturn(new ShardMeta());

        when(currentMetaManager.getSids(any(), any())).thenReturn("sids");
        when(dcMetaCache.getDownstreamDcs(any(), any(), any())).thenReturn(new HashSet<>(Arrays.asList("downstreamDc")));

        redisMetaChangeManager.handleClusterModified(comparator);

        verify(multiDcService, times(1)).sidsChange(any(), any(), any(), any());
    }
}