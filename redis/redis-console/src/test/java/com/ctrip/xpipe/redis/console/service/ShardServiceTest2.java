package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.ClusterMonitorModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEventListener;
import com.ctrip.xpipe.redis.console.service.impl.ShardServiceImpl;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.DalException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author: cchen6
 * 2020/10/13
 */
@RunWith(MockitoJUnitRunner.class)
public class ShardServiceTest2 extends AbstractConsoleTest {
    @InjectMocks
    private ShardServiceImpl shardService;
    @Mock
    private ShardTblDao shardTblDao;
    @Mock
    private ShardDao shardDao;
    @Mock
    private DcService dcService;
    @Mock
    private ClusterMetaModifiedNotifier notifier;
    @Mock
    private ClusterService clusterService;
    @Mock
    private SentinelService sentinelService;
    @Mock
    private MetaCache metaCache;
    @Mock
    private List<ShardEventListener> shardEventListeners;
    @Mock
    private ClusterMonitorModifiedNotifier monitorNotifier;

    @Mock
    private ConsoleConfig consoleConfig;
    private String clusterName = "clusterName";

    String shardName1 = "shard1";
    String shardName2 = "shard2";
    long shardId1 = 1;
    long shardId2 = 2;
    private List<String> shardNames = Lists.newArrayList(shardName1, shardName2);

    ShardTbl shardTbl1 = mock(ShardTbl.class);
    ShardTbl shardTbl2 = mock(ShardTbl.class);

    List<ShardTbl> shardTbls = Lists.newArrayList(shardTbl1, shardTbl2);
    DcTbl dcTbl1 = mock(DcTbl.class);
    DcTbl dcTbl2 = mock(DcTbl.class);
    String idc1 = "idc1";
    String idc2 = "idc2";

    List<DcTbl> dcTbls = Lists.newArrayList(dcTbl1, dcTbl2);
    @Mock
    ClusterTbl clusterTbl;

    @Before
    public void setUp() throws Exception {
        shardService.dao = shardTblDao;
        when(shardTbl1.getId()).thenReturn(shardId1);
        when(shardTbl2.getId()).thenReturn(shardId2);
        when(shardTbl1.getShardName()).thenReturn(shardName1);
        when(shardTbl2.getShardName()).thenReturn(shardName2);
        when(dcTbl1.getDcName()).thenReturn(idc1);
        when(dcTbl2.getDcName()).thenReturn(idc2);

        when(clusterService.find(clusterName))
                .thenReturn(clusterTbl);
        when(sentinelService.findByShard(anyLong()))
                .thenReturn(null);
        when(consoleConfig.shouldNotifyClusterTypes()).thenReturn(Sets.newHashSet(ClusterType.ONE_WAY.name(),ClusterType.BI_DIRECTION.name()));
    }

    @Test
    public void deleteShards() throws DalException {
        when(clusterTbl.getClusterName()).thenReturn(clusterName);
        when(clusterTbl.getClusterType()).thenReturn(ClusterType.ONE_WAY.name());
        when(shardTblDao.findByShardNames(clusterName, shardNames, ShardTblEntity.READSET_ID_NAME_AND_MONITOR_NAME))
                .thenReturn(shardTbls);
        shardService.deleteShards(clusterTbl, shardNames);
        verify(shardDao).deleteShardsBatch(shardTbls);
        verify(monitorNotifier).notifyClusterUpdate(anyString(), anyLong());
    }

    @Test
    public void deleteShardsFail1() throws Exception {
        DalException dalException = new DalException("deleteShardsBatch");
        doThrow(dalException).when(shardDao).deleteShardsBatch(shardTbls);
        try {
            shardService.deleteShards(clusterTbl, shardNames);
        } catch (ServerException e) {
            assertEquals(e.getMessage(), dalException.getMessage());
        }
    }
}
