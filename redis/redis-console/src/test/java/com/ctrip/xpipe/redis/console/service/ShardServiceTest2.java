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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils; // 2. 引入工具类
import org.unidal.dal.jdbc.DalException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author: cchen6
 * 2020/10/13
 */
@RunWith(MockitoJUnitRunner.class)
public class ShardServiceTest2  extends AbstractConsoleTest { // 1. 不再继承基类

    // 3. 不再使用 @InjectMocks，改为手动创建和注入
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
    private SentinelGroupService sentinelService;
    @Mock
    private MetaCache metaCache;
    @Mock
    private List<ShardEventListener> shardEventListeners;
    @Mock
    private ClusterMonitorModifiedNotifier monitorNotifier;
    @Mock
    private ConsoleConfig consoleConfig;

    private String clusterName = "clusterName";
    private String shardName1 = "shard1";
    private String shardName2 = "shard2";
    private long shardId1 = 1;
    private long shardId2 = 2;
    private List<String> shardNames = Lists.newArrayList(shardName1, shardName2);

    private ShardTbl shardTbl1 = mock(ShardTbl.class);
    private ShardTbl shardTbl2 = mock(ShardTbl.class);
    private List<ShardTbl> shardTbls = Lists.newArrayList(shardTbl1, shardTbl2);

    @Mock
    private ClusterTbl clusterTbl;

    @Before
    public void setUp() throws Exception {
        // 4. 手动创建被测试对象
        shardService = new ShardServiceImpl();

        // 5. 手动注入所有依赖
        ReflectionTestUtils.setField(shardService, "dao", shardTblDao); // 注入父类的 dao 字段
        ReflectionTestUtils.setField(shardService, "shardDao", shardDao);
        ReflectionTestUtils.setField(shardService, "dcService", dcService);
        ReflectionTestUtils.setField(shardService, "notifier", notifier);
        ReflectionTestUtils.setField(shardService, "clusterService", clusterService);
        ReflectionTestUtils.setField(shardService, "sentinelService", sentinelService);
        ReflectionTestUtils.setField(shardService, "metaCache", metaCache);
        ReflectionTestUtils.setField(shardService, "shardEventListeners", shardEventListeners);
        ReflectionTestUtils.setField(shardService, "monitorNotifier", monitorNotifier);
        ReflectionTestUtils.setField(shardService, "consoleConfig", consoleConfig);

        // 设置 Mock 行为
        when(clusterTbl.getClusterName()).thenReturn(clusterName);
        when(shardTbl1.getId()).thenReturn(shardId1);
        when(shardTbl2.getId()).thenReturn(shardId2);
//        when(sentinelService.findByShard(anyLong()))
//                .thenReturn(null);
        when(shardTblDao.findByShardNames(clusterName, shardNames, ShardTblEntity.READSET_NAME_AND_MONITOR_NAME))
                .thenReturn(shardTbls);
        when(consoleConfig.shouldNotifyClusterTypes()).thenReturn(Sets.newHashSet(ClusterType.ONE_WAY.name(), ClusterType.BI_DIRECTION.name()));
        when(metaCache.anyDcMigratable(anyString())).thenReturn(true);
    }

    @Test
    public void deleteShards() throws DalException {
        when(clusterTbl.getClusterType()).thenReturn(ClusterType.ONE_WAY.name());
        when(shardTblDao.findByShardNames(clusterName, shardNames, ShardTblEntity.READSET_NAME_AND_MONITOR_NAME))
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
            Assert.fail();
        } catch (ServerException e) {
            assertEquals(e.getMessage(), dalException.getMessage());
        }
    }
}
