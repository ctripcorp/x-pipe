package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.Collections;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Jan 29, 2018
 */
public class MetaUpdateTest2 {

    @Mock
    private RedisService redisService;

    @Mock
    private KeeperAdvancedService keeperAdvancedService;

    @Mock
    private DcClusterService dcClusterService;

    @Mock
    private AzGroupClusterRepository azGroupClusterRepository;

    @Mock
    private AzGroupCache azGroupCache;

    @Spy
    @InjectMocks
    private MetaUpdate metaUpdate = new MetaUpdate();

    List<KeeperBasicInfo> keepers;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

    }

    @Test
    public void addKeepers1() throws Exception {
        String cluster = "cluster-test", shard = "shard-test", dc = "SHAJQ";
        when(redisService.findKeepersByDcClusterShard(dc, cluster, shard)).thenReturn(null);

        KeeperBasicInfo keeper1 = new KeeperBasicInfo();
        KeeperBasicInfo keeper2 = new KeeperBasicInfo();
        keeper1.setHost("127.0.0.1");
        keeper1.setPort(6379);
        keeper1.setKeeperContainerId(1);
        keeper2.setHost("127.0.0.1");
        keeper2.setPort(6380);
        keeper2.setKeeperContainerId(1);

        List<KeeperBasicInfo> keepers = Lists.newArrayList(keeper1, keeper2);
        when(keeperAdvancedService.findBestKeepers(eq(dc), eq(KEEPER_PORT_DEFAULT), any(), eq(cluster)))
                .thenReturn(keepers);

        when(redisService.insertKeepers(dc, cluster, shard, keepers)).thenReturn(2);

        Assert.assertEquals(2, metaUpdate.doAddKeepers(dc, cluster, new ShardTbl().setShardName(shard), dc));
    }

    @Test
    public void addKeepers2() throws Exception {
        String cluster = "cluster-test", shard = "shard-test", dc = "SHAJQ";
        when(redisService.findKeepersByDcClusterShard(dc, cluster, shard)).thenReturn(null);

        KeeperBasicInfo keeper1 = new KeeperBasicInfo();
        keeper1.setHost("127.0.0.1");
        keeper1.setPort(6379);
        keeper1.setKeeperContainerId(1);

        keepers = Lists.newArrayList(keeper1);
        when(keeperAdvancedService.findBestKeepers(eq(dc), eq(KEEPER_PORT_DEFAULT), any(), eq(cluster)))
                .thenReturn(keepers);

        when(redisService.insertKeepers(anyString(), anyString(), anyString(), eq(keepers)))
                .thenReturn(2 - keepers.size());
        Assert.assertEquals(1, metaUpdate.doAddKeepers(dc, cluster, new ShardTbl().setShardName(shard), dc));


    }

    @Test
    public void addKeepersWithIsDRMasterDc() throws Exception {
        ClusterTbl clusterTbl = mock(ClusterTbl.class);
        when(clusterTbl.getClusterName()).thenReturn("cluster-test");
        when(clusterTbl.getId()).thenReturn(1L);
        // TODO: 2022/10/10 remove hetero
//        when(clusterTbl.getClusterType()).thenReturn(ClusterType.HETERO.toString());
        when(clusterTbl.getClusterType()).thenReturn(ClusterType.ONE_WAY.toString());
        when(clusterTbl.getId()).thenReturn(1L);
        DcClusterTbl dcClusterTbl = mock(DcClusterTbl.class);
//        when(dcClusterTbl.getGroupType()).thenReturn(DcGroupType.DR_MASTER.toString());
        when(dcClusterTbl.getDcId()).thenReturn(2L);
        when(dcClusterService.find("SHAJQ", "cluster-test")).thenReturn(dcClusterTbl);
        when(azGroupClusterRepository.selectByClusterId(any())).thenReturn(Collections.emptyList());
        ShardTbl shardTbl = mock(ShardTbl.class);
        RedisCreateInfo redisCreateInfo = mock(RedisCreateInfo.class);
        when(redisCreateInfo.getDcId()).thenReturn("SHAJQ");

        metaUpdate.addKeepers(clusterTbl, shardTbl, Lists.newArrayList(redisCreateInfo));
        verify(metaUpdate).doAddKeepers("SHAJQ", "cluster-test", shardTbl, "SHAJQ");
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void valvalidateRedisCreateInfo() throws Exception {

        metaUpdate.validateRedisCreateInfo(Lists.newArrayList(
                new RedisCreateInfo().setDcId("jq").setRedises("127.0.0.1:6379"),
                new RedisCreateInfo().setDcId("jq").setRedises("127.0.0.2:6380")));

    }

}