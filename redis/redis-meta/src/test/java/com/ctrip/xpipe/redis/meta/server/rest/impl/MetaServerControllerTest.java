package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardCurrentMetaModel;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetaServerControllerTest {

    @Mock
    private MetaServer metaServer;

    @Mock
    private DcMetaCache dcMetaCache;

    private MetaServerController controller;

    @Before
    public void setUp() {
        controller = new MetaServerController();
        ReflectionTestUtils.setField(controller, "currentMetaServer", metaServer);
        ReflectionTestUtils.setField(controller, "dcMetaCache", dcMetaCache);
    }

    @Test
    public void testGetShardCurrentMetaUsesSurviveKeeperRuntimeAddressDirectly() {
        long clusterDbId = 1L;
        long shardDbId = 2L;
        String clusterName = "cluster";
        String shardName = "shard";
        KeeperMeta keeper = new KeeperMeta().setIp("10.2.3.15").setHostname("keeper-pod-a-0")
                .setPort(6380).setKeeperContainerId(1001L).setActive(true);
        ShardMeta shardMeta = new ShardMeta().setId(shardName).setDbId(shardDbId);
        ClusterMeta clusterMeta = new ClusterMeta().setId(clusterName).setDbId(clusterDbId).addShard(shardMeta);
        Pair<String, Integer> keeperMaster = Pair.of("10.0.0.1", 6379);

        when(dcMetaCache.clusterShardId2DbId(clusterName, shardName)).thenReturn(Pair.of(clusterDbId, shardDbId));
        when(dcMetaCache.getClusterType(clusterDbId)).thenReturn(ClusterType.ONE_WAY);
        when(dcMetaCache.getClusterMeta(clusterDbId)).thenReturn(clusterMeta);
        when(metaServer.getOneWaySurviveKeepers(clusterName, shardName))
                .thenReturn(Collections.singletonList(keeper));
        when(metaServer.getKeeperMaster(clusterName, shardName)).thenReturn(keeperMaster);

        ShardCurrentMetaModel result = controller.getShardCurrentMeta(clusterName, shardName);

        Assert.assertEquals(Collections.singletonList(keeper), result.getSurviveKeepers());
        Assert.assertEquals("10.2.3.15", result.getSurviveKeepers().get(0).getIp());
        Assert.assertEquals("keeper-pod-a-0", result.getSurviveKeepers().get(0).getHostname());
        Assert.assertEquals(shardDbId, result.getShardDbId());
        Assert.assertEquals(shardMeta, result.getShardMeta());
        Assert.assertEquals(keeperMaster, result.getKeeperMaster());
    }
}
