package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MetaUpdateTest4 {
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
    ;

    private String clusterName = "meta-update-shard-test";

    private String shardName = "shard";

    private String dc1 = "dc1";

    private String dc2 = "dc2";


    @Test
    public void testSyncDeleteShard() {
        DcTbl dcTbl1 = mock(DcTbl.class);
        DcTbl dcTbl2 = mock(DcTbl.class);
        List<DcTbl> dcTblList = Lists.newArrayList(dcTbl1, dcTbl2);

        when(dcTbl1.getDcName())
                .thenReturn(dc1);
        when(dcTbl2.getDcName())
                .thenReturn(dc2);

        when(clusterService.getClusterRelatedDcs(clusterName))
                .thenReturn(dcTblList);

        MetaServerConsoleService metaServerConsoleService1 = mock(MetaServerConsoleService.class);
        MetaServerConsoleService metaServerConsoleService2 = mock(MetaServerConsoleService.class);

        when(metaServerConsoleServiceManagerWrapper.get(dc1))
                .thenReturn(metaServerConsoleService1);
        when(metaServerConsoleServiceManagerWrapper.get(dc2))
                .thenReturn(metaServerConsoleService2);

        ClusterMeta clusterMeta1 = mock(ClusterMeta.class);
        ClusterMeta clusterMeta2 = mock(ClusterMeta.class);

        when(clusterMetaService.getClusterMeta(dc1, clusterName))
                .thenReturn(clusterMeta1);
        when(clusterMetaService.getClusterMeta(dc2, clusterName))
                .thenReturn(clusterMeta2);

        metaUpdate.syncDeleteShard(clusterName, shardName);
        verify(shardService).deleteShard(clusterName, shardName);
        verify(metaServerConsoleService1).clusterModified(clusterName, clusterMeta1);
        verify(metaServerConsoleService2).clusterModified(clusterName, clusterMeta2);
    }
}
