package com.ctrip.xpipe.redis.console.healthcheck.nonredis.sentinelconfig;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class SentinelConfigCheckTest {

    @InjectMocks
    SentinelConfigCheck sentinelConfigCheck;

    @Mock
    MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private ConsoleDbConfig consoleDbConfig;

    @Mock
    private ConsoleConfig consoleConfig;

    private ClusterType mockClusterType = ClusterType.ONE_WAY;

    private List<String> mockDcs = Arrays.asList("jq", "oy", "fra");

    private List<String> mockClusters = Arrays.asList("cluster1", "cluster2");

    private List<String> mockShards = Arrays.asList("shard1", "shard2");

    private Map<String, String> activeDcMap = new HashMap<String, String>() {{
        put("cluster1", "jq");
        put("cluster2", "fra");
    }};

    private List<Set<String> > regions = Arrays.asList(
            Sets.newHashSet("jq", "oy"),
            Sets.newHashSet("fra")
    );

    private Map<String, Set<String> > expectedUnsafeClusters = new HashMap<String, Set<String> >() {{
        put("jq", Sets.newHashSet("cluster1"));
        put("oy", Sets.newHashSet("cluster1"));
        put("fra", Sets.newHashSet("cluster2"));
    }};

    @Before
    public void beforeSentinelConfigCheckTest() {
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
        when(consoleDbConfig.sentinelCheckWhiteList(Mockito.anyBoolean())).thenReturn(Collections.emptySet());
        when(metaCache.getActiveDc(Mockito.anyString())).then(invocationOnMock -> {
            String cluster = invocationOnMock.getArgument(0, String.class);
            return activeDcMap.get(cluster);
        });

        when(metaCache.isCrossRegion(Mockito.anyString(), Mockito.anyString())).then(invocationOnMock -> {
            String activeDc = invocationOnMock.getArgument(0, String.class);
            String backupDc = invocationOnMock.getArgument(1, String.class);
            for (Set<String> dcSet: regions) {
                if (dcSet.contains(activeDc)) return !dcSet.contains(backupDc);
            }

            return false;
        });
    }

    @Test
    public void testDoCheckWithOneWayCluster() {
        Mockito.doAnswer(invocationOnMock -> {
           String dc = invocationOnMock.getArgument(0, String.class);
           String cluster = invocationOnMock.getArgument(1, String.class);
           String shard = invocationOnMock.getArgument(2, String.class);

           Assert.assertTrue(expectedUnsafeClusters.get(dc).contains(cluster));
           Assert.assertTrue(mockShards.contains(shard));

           return null;
        }).when(alertManager).alert(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(), Mockito.any(), Mockito.any());

        when(consoleConfig.supportSentinelHealthCheck(any(),any())).thenReturn(false);
        sentinelConfigCheck.doAction();

        Mockito.verify(alertManager, never())
                .alert(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                        Mockito.any(), Mockito.any(), Mockito.anyString());

        when(consoleConfig.supportSentinelHealthCheck(any(),any())).thenReturn(true);
        sentinelConfigCheck.doAction();

        Mockito.verify(alertManager, Mockito.times(6))
                .alert(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                        Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testDoCheckWithBiDirectionCluster() {
        this.mockClusterType = ClusterType.BI_DIRECTION;
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
        when(consoleConfig.supportSentinelHealthCheck(any(),any())).thenReturn(true);
        sentinelConfigCheck.doAction();

        Mockito.verify(alertManager, Mockito.times(12))
                .alert(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                        Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testCheckWithSentinelCheckWhitelist() {
        when(consoleDbConfig.sentinelCheckWhiteList(Mockito.anyBoolean())).thenReturn(new HashSet<>(mockClusters));
        sentinelConfigCheck.doAction();
        Mockito.verify(alertManager, Mockito.never())
                .alert(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                        Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testIsDcClusterShardSafe() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        xpipeMeta.getDcs().get("fra").addCluster(mockHeteroClusterMeta("cluster4", ClusterType.HETERO.name(),  ClusterType.SINGLE_DC.name(), "fra"));
        xpipeMeta.getDcs().get("oy").addCluster(mockHeteroClusterMeta("cluster4", ClusterType.HETERO.name(),  ClusterType.SINGLE_DC.name(), "jq"));

        Mockito.reset(metaCache);
        buildForHeteroTest();

        DcMeta oyDcMeta = xpipeMeta.getDcs().get("oy");
        ClusterMeta cluster4Meta = oyDcMeta.getClusters().get("cluster4");
        ShardMeta shard4Meta = cluster4Meta.getShards().get("shard1");

        try {
            sentinelConfigCheck.isDcClusterShardSafe(oyDcMeta, cluster4Meta, shard4Meta);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MetaException);
            System.out.println(e.getMessage());
        }
    }

    private ClusterMeta mockHeteroClusterMeta(String name, String type, String azGroupType, String activeDc) {

        ClusterMeta cluster = new ClusterMeta().setId(name).setType(type).addShard(mockShardMeta("shard1")).addShard(mockShardMeta("shard2"));
        cluster.setAzGroupType(azGroupType);
        cluster.setActiveDc(activeDc);

        return cluster;
    }

    public void buildForHeteroTest() {
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
        when(consoleDbConfig.sentinelCheckWhiteList(Mockito.anyBoolean())).thenReturn(Collections.emptySet());
        when(metaCache.getActiveDc(Mockito.anyString())).then(invocationOnMock -> {
            String cluster = invocationOnMock.getArgument(0, String.class);
            if (cluster.equals("cluster4")) {
                throw new MetaException("cluster4 is hetero cluster without one way");
            }
            return activeDcMap.get(cluster);
        });

        when(metaCache.isCrossRegion(Mockito.anyString(), Mockito.anyString())).then(invocationOnMock -> {
            String activeDc = invocationOnMock.getArgument(0, String.class);
            String backupDc = invocationOnMock.getArgument(1, String.class);
            for (Set<String> dcSet: regions) {
                if (dcSet.contains(activeDc)) return !dcSet.contains(backupDc);
            }

            return false;
        });
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

        for (String cluster: mockClusters) {
            dcMeta.addCluster(mockClusterMeta(cluster));
        }

        return dcMeta;
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
        shardMeta.setSentinelId(0L);
        return shardMeta;
    }

}
