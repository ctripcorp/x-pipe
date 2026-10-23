package com.ctrip.xpipe.redis.console.healthcheck.nonredis.availablezone;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperAvailableZoneCheckTest {
    @InjectMocks
    KeeperAvailableZoneCheck keeperAvailableZoneCheck;

    @Mock
    MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Captor
    ArgumentCaptor<String> clusterCaptor;

    @Captor
    ArgumentCaptor<String> shardCaptor;

    @Captor
    ArgumentCaptor<String> dcCaptor;

    private List<String> mockDcs = Arrays.asList("jq", "oy", "fra");

    private List<String> mockClusters = Arrays.asList("cluster1", "cluster2");

    private List<String> mockShards = Arrays.asList("shard1", "shard2");

    private List<String> keepercontainerIps = Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4");

    /** 与 keepercontainerIps 一一对应；关联键是 keepercontainer_id，不是 IP。 */
    private List<Long> keepercontainerIds = Arrays.asList(1L, 2L, 3L, 4L);

    private List<String> azIds = Arrays.asList("1", "2");


    @Before
    public void beforeKeeperAvailableZoneCheckTest() {
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
    }

    @Test
    public void testKeeperAvailableZoneCheck() {
        keeperAvailableZoneCheck.doAction();
        Mockito.verify(alertManager, Mockito.times(1))
                .alert(dcCaptor.capture(), clusterCaptor.capture(), shardCaptor.capture(), Mockito.any(), Mockito.any(), Mockito.anyString());

        List<String> expectdcs = dcCaptor.getAllValues();
        Assert.assertEquals(Arrays.asList(mockDcs.get(2)), expectdcs);
        List<String> expectClusters = clusterCaptor.getAllValues();
        Assert.assertEquals(Arrays.asList(mockClusters.get(0)), expectClusters);
        List<String> expectShards = shardCaptor.getAllValues();
        Assert.assertEquals(Arrays.asList(mockShards.get(0)), expectShards);

    }


    /**
     * 容器 Keeper 的 redis_ip 与 keepercontainer_ip 都是占位值 0.0.0.0。以 IP 关联时所有 pod
     * 会被归并成一台机器，判定恒成立；按 keepercontainer_id 关联才能真正区分可用区。
     */
    @Test
    public void testContainerKeepersSharingPlaceholderIp() {
        String placeholder = "0.0.0.0";
        DcMeta dcMeta = new DcMeta().setId("jq");
        dcMeta.addAz(new AzMeta(azIds.get(0)));
        dcMeta.addAz(new AzMeta(azIds.get(1)));
        dcMeta.addKeeperContainer(placeholderIpContainer(10L, 1L, placeholder));
        dcMeta.addKeeperContainer(placeholderIpContainer(11L, 1L, placeholder));
        dcMeta.addKeeperContainer(placeholderIpContainer(12L, 2L, placeholder));

        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster1");
        clusterMeta.setType(ClusterType.ONE_WAY.toString());
        // 同一可用区的两个容器 → 应告警
        clusterMeta.addShard(placeholderIpShard("sameAz", placeholder, 10L, 11L));
        // 跨可用区 → 不应告警
        clusterMeta.addShard(placeholderIpShard("crossAz", placeholder, 10L, 12L));
        dcMeta.addCluster(clusterMeta);

        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(dcMeta));

        keeperAvailableZoneCheck.doAction();

        Mockito.verify(alertManager, Mockito.times(1)).alert(Mockito.anyString(), Mockito.anyString(),
                shardCaptor.capture(), Mockito.any(), Mockito.any(), Mockito.anyString());
        Assert.assertEquals(Arrays.asList("sameAz"), shardCaptor.getAllValues());
    }

    /** 可用区未知不能当作一个真实可用区参与去重，否则两个未知会被误判为「在同一可用区」。 */
    @Test
    public void testUnknownAvailableZoneDoesNotAlert() {
        DcMeta dcMeta = new DcMeta().setId("jq");
        dcMeta.addAz(new AzMeta(azIds.get(0)));
        dcMeta.addAz(new AzMeta(azIds.get(1)));
        // 容器不在本 dc 的 keeperContainers 里，以及 DB 未配 az
        dcMeta.addKeeperContainer(placeholderIpContainer(20L, null, "0.0.0.0"));

        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster1");
        clusterMeta.setType(ClusterType.ONE_WAY.toString());
        clusterMeta.addShard(placeholderIpShard("unknownAz", "0.0.0.0", 20L, 99L));
        dcMeta.addCluster(clusterMeta);

        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(dcMeta));

        keeperAvailableZoneCheck.doAction();

        Mockito.verify(alertManager, Mockito.never()).alert(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    private KeeperContainerMeta placeholderIpContainer(Long id, Long azId, String ip) {
        KeeperContainerMeta meta = new KeeperContainerMeta();
        meta.setId(id);
        meta.setAzId(azId);
        meta.setIp(ip);
        return meta;
    }

    private ShardMeta placeholderIpShard(String shardId, String ip, Long... containerIds) {
        ShardMeta shardMeta = new ShardMeta().setId(shardId);
        for (Long containerId : containerIds) {
            KeeperMeta keeperMeta = new KeeperMeta();
            keeperMeta.setIp(ip);
            keeperMeta.setKeeperContainerId(containerId);
            shardMeta.addKeeper(keeperMeta);
        }
        return shardMeta;
    }

    private XpipeMeta mockXpipeMeta() {
        XpipeMeta meta = new XpipeMeta();

        for (String dc: mockDcs) {
            meta.addDc(mockDcMeta(dc));
        }

        return meta;
    }

    private DcMeta mockDcMeta(String dc) {
        DcMeta dcMeta = new DcMeta();
        dcMeta.setId(dc);
        if(dc.equals(mockDcs.get(2))) {
            dcMeta.addAz(new AzMeta(azIds.get(0)));
            dcMeta.addAz(new AzMeta(azIds.get(1)));

            dcMeta.addKeeperContainer(mockKeepercontainerMeta(0, Long.valueOf(azIds.get(0))));
            dcMeta.addKeeperContainer(mockKeepercontainerMeta(1, Long.valueOf(azIds.get(0))));
            dcMeta.addKeeperContainer(mockKeepercontainerMeta(2, Long.valueOf(azIds.get(1))));
            dcMeta.addKeeperContainer(mockKeepercontainerMeta(3, Long.valueOf(azIds.get(1))));
        } else if (dc.equals(mockDcs.get(1))){
            dcMeta.addAz(new AzMeta(azIds.get(0)));
            dcMeta.addKeeperContainer(mockKeepercontainerMeta(0, Long.valueOf(azIds.get(0))));
            dcMeta.addKeeperContainer(mockKeepercontainerMeta(1, Long.valueOf(azIds.get(0))));
        }

        for (String cluster: mockClusters) {
            dcMeta.addCluster(mockClusterMeta(cluster));
        }

        return dcMeta;
    }

    private KeeperContainerMeta mockKeepercontainerMeta(int index, Long azId) {
        KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
        keeperContainerMeta.setId(keepercontainerIds.get(index));
        keeperContainerMeta.setAzId(azId);
        keeperContainerMeta.setIp(keepercontainerIps.get(index));
        return keeperContainerMeta;
    }

    private ClusterMeta mockClusterMeta(String cluster) {
        ClusterMeta clusterMeta = new ClusterMeta();
        clusterMeta.setId(cluster);
        if(cluster.equals(mockClusters.get(0)))
            clusterMeta.setType(ClusterType.ONE_WAY.toString());
        else
            clusterMeta.setType(ClusterType.BI_DIRECTION.toString());


        for (String shard: mockShards) {
            clusterMeta.addShard(mockShardMeta(shard));
        }

        return clusterMeta;
    }

    private ShardMeta mockShardMeta(String shard) {
        ShardMeta shardMeta = new ShardMeta();
        shardMeta.setId(shard);
        if(shard.equals(mockShards.get(0))) {
            // 两个 Keeper 都在 az1 的容器上 —— 唯一应告警的分片
            shardMeta.addKeeper(mockKeeperMeta(0));
            shardMeta.addKeeper(mockKeeperMeta(1));
        } else {
            shardMeta.addKeeper(mockKeeperMeta(1));
            shardMeta.addKeeper(mockKeeperMeta(3));
        }
        return shardMeta;
    }

    private KeeperMeta mockKeeperMeta(int index) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp(keepercontainerIps.get(index));
        keeperMeta.setKeeperContainerId(keepercontainerIds.get(index));
        return keeperMeta;
    }
}
