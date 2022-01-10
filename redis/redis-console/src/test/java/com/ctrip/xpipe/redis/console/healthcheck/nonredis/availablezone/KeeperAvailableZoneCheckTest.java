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

import java.util.*;

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


    private ClusterType mockClusterType = ClusterType.ONE_WAY;

    private List<String> mockDcs = Arrays.asList("jq", "oy", "fra");

    private List<String> mockClusters = Arrays.asList("cluster1", "cluster2");

    private List<String> mockShards = Arrays.asList("shard1", "shard2");

    private List<String> keepercontainerIps = Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4");

    private List<String> azIds = Arrays.asList("1", "2");


    @Before
    public void beforeKeeperAvailableZoneCheckTest() {
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
    }

    @Test
    public void testKeeperAvailableZoneCheck() {
        keeperAvailableZoneCheck.doCheck();
        Mockito.verify(alertManager, Mockito.times(2))
                .alert(dcCaptor.capture(), clusterCaptor.capture(), shardCaptor.capture(), Mockito.any(), Mockito.any(), Mockito.anyString());

        List<String> expectdcs = dcCaptor.getAllValues();
        Assert.assertEquals(Arrays.asList(mockDcs.get(2), mockDcs.get(2)), expectdcs);
        List<String> expectClusters = clusterCaptor.getAllValues();
        Assert.assertEquals(mockClusters, expectClusters);
        List<String> expectShards = shardCaptor.getAllValues();
        Assert.assertEquals(Arrays.asList(mockShards.get(0), mockShards.get(0)), expectShards);

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

            dcMeta.addKeeperContainer(mockKeepercontainerMeta(keepercontainerIps.get(0), Integer.valueOf(azIds.get(0))));
            dcMeta.addKeeperContainer(mockKeepercontainerMeta(keepercontainerIps.get(1), Integer.valueOf(azIds.get(0))));
            dcMeta.addKeeperContainer(mockKeepercontainerMeta(keepercontainerIps.get(2), Integer.valueOf(azIds.get(1))));
            dcMeta.addKeeperContainer(mockKeepercontainerMeta(keepercontainerIps.get(3), Integer.valueOf(azIds.get(1))));
        }

        for (String cluster: mockClusters) {
            dcMeta.addCluster(mockClusterMeta(cluster));
        }

        return dcMeta;
    }

    private KeeperContainerMeta mockKeepercontainerMeta(String ip, Integer azId) {
        KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
        keeperContainerMeta.setAzId(azId);
        keeperContainerMeta.setIp(ip);
        return keeperContainerMeta;
    }

    private ClusterMeta mockClusterMeta(String cluster) {
        ClusterMeta clusterMeta = new ClusterMeta();
        clusterMeta.setId(cluster);
        clusterMeta.setType(mockClusterType.toString());

        for (String shard: mockShards) {
            clusterMeta.addShard(mockShardMeta(shard));
        }

        return clusterMeta;
    }

    private ShardMeta mockShardMeta(String shard) {
        ShardMeta shardMeta = new ShardMeta();
        shardMeta.setId(shard);
        if(shard.equals(mockShards.get(0))) {
            shardMeta.addKeeper(mockKeeperMeta(keepercontainerIps.get(0)));
            shardMeta.addKeeper(mockKeeperMeta(keepercontainerIps.get(1)));
        } else {
            shardMeta.addKeeper(mockKeeperMeta(keepercontainerIps.get(1)));
            shardMeta.addKeeper(mockKeeperMeta(keepercontainerIps.get(3)));
        }
        return shardMeta;
    }

    private KeeperMeta mockKeeperMeta(String ip) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp(ip);
        return keeperMeta;
    }



}
