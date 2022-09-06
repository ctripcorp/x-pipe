package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ReplDirectionCreateInfo;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ReplDirectionTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ApplierService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ReplDirectionService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import static com.ctrip.xpipe.redis.checker.controller.result.RetMessage.FAIL_STATE;
import static com.ctrip.xpipe.redis.checker.controller.result.RetMessage.SUCCESS_STATE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MetaUpdateTest4 {
    @Spy
    @InjectMocks
    private MetaUpdate metaUpdate;

    @Mock
    private ShardService shardService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ClusterMetaService clusterMetaService;

    @Mock
    private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;

    @Mock
    private ReplDirectionService replDirectionService;

    private Logger logger = LoggerFactory.getLogger(MetaUpdateTest4.class);

    private String clusterName = "meta-update-shard-test";

    private String shardName1 = "shard1";
    private String shardName2 = "shard2";

    private String dc1 = "dc1";

    private String dc2 = "dc2";

    DcTbl dcTbl1 = mock(DcTbl.class);
    DcTbl dcTbl2 = mock(DcTbl.class);
    List<DcTbl> dcTblList = Lists.newArrayList(dcTbl1, dcTbl2);

    ClusterMeta clusterMeta1 = mock(ClusterMeta.class);
    ClusterMeta clusterMeta2 = mock(ClusterMeta.class);

    ShardTbl shardTbl1 = mock(ShardTbl.class);
    ShardTbl shardTbl2 = mock(ShardTbl.class);

    @Mock
    ClusterTbl clusterTbl;

    long clusterId = 100;

    long dcId1 = 1000;
    long dcId2 = 1001;

    MetaServerConsoleService metaServerConsoleService1 = mock(MetaServerConsoleService.class);
    MetaServerConsoleService metaServerConsoleService2 = mock(MetaServerConsoleService.class);

    List<ShardTbl> shardTbls = Lists.newArrayList(shardTbl1, shardTbl2);

    List<String> shardNames = Lists.newArrayList(shardName1, shardName2);

    @Before
    public void setUp() throws Exception {
        when(dcTbl1.getDcName())
                .thenReturn(dc1);
        when(dcTbl2.getDcName())
                .thenReturn(dc2);
        when(dcTbl1.getId())
                .thenReturn(dcId1);
        when(dcTbl2.getId())
                .thenReturn(dcId2);
        when(clusterTbl.getId())
                .thenReturn(clusterId);
        when(clusterTbl.getClusterName()).thenReturn(clusterName);
        when(shardTbl1.getShardName()).thenReturn(shardName1);
        when(shardTbl2.getShardName()).thenReturn(shardName2);

        when(clusterService.getClusterRelatedDcs(clusterName))
                .thenReturn(dcTblList);
        when(metaServerConsoleServiceManagerWrapper.get(dc1))
                .thenReturn(metaServerConsoleService1);
        when(metaServerConsoleServiceManagerWrapper.get(dc2))
                .thenReturn(metaServerConsoleService2);
        when(clusterService.find(clusterName))
                .thenReturn(clusterTbl);
        when(shardService.findAllByClusterName(clusterName))
                .thenReturn(shardTbls);
        when(clusterMetaService.getClusterMeta(dc1, clusterName))
                .thenReturn(clusterMeta1);
        when(clusterMetaService.getClusterMeta(dc2, clusterName))
                .thenReturn(clusterMeta2);
    }

    @Test
    public void syncBatchDeleteShards() {
        metaUpdate.syncBatchDeleteShards(clusterName, Lists.newArrayList(shardName1, shardName2));
        verify(shardService).deleteShards(clusterTbl, shardNames);
        verify(metaServerConsoleService1).clusterModified(clusterName, clusterMeta1);
        verify(metaServerConsoleService2).clusterModified(clusterName, clusterMeta2);
    }

    @Test
    public void syncBatchDeleteShardsFail1() {
        doThrow(new RejectedExecutionException("deleteShards")).when(shardService).deleteShards(clusterTbl, shardNames);
        try {
            RetMessage retMessage = metaUpdate.syncBatchDeleteShards(clusterName, Lists.newArrayList(shardName1, shardName2));
            assertEquals(retMessage.getState(), FAIL_STATE);
        } catch (Throwable t) {
            fail();
        }
    }

    @Test
    public void syncBatchDeleteShardsFail2() {
        when(shardService.findAllByClusterName(clusterName)).thenThrow(new ServerException("findAllShardNamesByClusterName"));
        try {
            RetMessage retMessage = metaUpdate.syncBatchDeleteShards(clusterName, Lists.newArrayList(shardName1, shardName2));
            assertEquals(retMessage.getState(), FAIL_STATE);
        } catch (Throwable t) {
            fail();
        }
    }

    @Test
    public void syncBatchDeleteShardsFail3() {
        when(shardService.findAllByClusterName(clusterName)).thenReturn(Lists.newArrayList());
        try {
            RetMessage retMessage = metaUpdate.syncBatchDeleteShards(clusterName, Lists.newArrayList(shardName1, shardName2));
            assertEquals(retMessage.getState(), SUCCESS_STATE);
            verify(shardService, never()).deleteShards(clusterTbl, shardNames);
            verify(metaServerConsoleService1, never()).clusterModified(clusterName, clusterMeta1);
            verify(metaServerConsoleService2, never()).clusterModified(clusterName, clusterMeta2);
        } catch (Throwable t) {
            fail();
        }
    }

    @Test
    public void syncBatchDeleteShardsFail4() {
        doThrow(new RuntimeException("clusterModified")).when(metaServerConsoleService1).clusterModified(anyString(), any());
        try {
            RetMessage retMessage = metaUpdate.syncBatchDeleteShards(clusterName, Lists.newArrayList(shardName1, shardName2));
            verify(shardService).deleteShards(clusterTbl, shardNames);
            assertEquals(retMessage.getState(), FAIL_STATE);
        } catch (Throwable t) {
            fail();
        }
    }

    @Test
    public void syncBatchDeleteShardsFail5() {
        when(shardService.findAllByClusterName(clusterName)).thenReturn(Lists.newArrayList(shardTbl1));
        try {
            RetMessage retMessage = metaUpdate.syncBatchDeleteShards(clusterName, Lists.newArrayList(shardName1, shardName2));
            assertEquals(retMessage.getState(), SUCCESS_STATE);
            logger.info("{}", retMessage.getMessage());
            verify(shardService, never()).deleteShards(clusterTbl, shardNames);
            verify(metaServerConsoleService1, never()).clusterModified(clusterName, clusterMeta1);
            verify(metaServerConsoleService2, never()).clusterModified(clusterName, clusterMeta2);
        } catch (Throwable t) {
            fail();
        }
    }

    @Test
    public void syncBatchDeleteShardsFail6() {
        doReturn(null).when(clusterService).find(clusterName);
        try {
            RetMessage retMessage = metaUpdate.syncBatchDeleteShards(clusterName, Lists.newArrayList(shardName1, shardName2));
            assertEquals(retMessage.getState(), SUCCESS_STATE);
            logger.info("{}", retMessage.getMessage());
            verify(shardService, never()).deleteShards(clusterTbl, shardNames);
            verify(metaServerConsoleService1, never()).clusterModified(clusterName, clusterMeta1);
            verify(metaServerConsoleService2, never()).clusterModified(clusterName, clusterMeta2);
        } catch (Throwable t) {
            fail();
        }
    }

    @Test
    public void addReplDirections() {
        long replDirectionId = 10;
        ReplDirectionCreateInfo replDirectionCreateInfo1 = new ReplDirectionCreateInfo().setFromDcName(dc1).setSrcDcName(dc1).setToDcName(dc2);

        when(shardService.findAllShardByDcCluster(dcTbl1.getId(), clusterTbl.getId())).thenReturn(Lists.newArrayList(shardTbl1));

        ReplDirectionTbl replDirectionTbl = new ReplDirectionTbl().setId(replDirectionId).setSrcDcId(dcId1);
        doAnswer(invocationOnMock -> replDirectionTbl).when(replDirectionService).addReplDirectionByInfoModel(anyString(), any());
        doAnswer(invocationOnMock -> null).when(metaUpdate).addAppliers(anyString(), anyString(), any(), anyLong());

        metaUpdate.createReplDirections(clusterName, Lists.newArrayList(replDirectionCreateInfo1));
        verify(metaUpdate).addAppliers(dc2, clusterName, shardTbl1, replDirectionId);
        verify(metaUpdate, never()).addAppliers(dc2, clusterName, shardTbl2, replDirectionId);
    }
}
