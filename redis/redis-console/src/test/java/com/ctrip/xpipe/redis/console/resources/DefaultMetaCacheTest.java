package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.TooManyClustersRemovedException;
import com.ctrip.xpipe.redis.console.exception.TooManyDcsRemovedException;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.impl.RedisCheckRuleServiceImpl;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.unidal.tuple.Triple;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultMetaCacheTest extends AbstractRedisTest {

    @Mock
    private DcMetaService dcMetaService;

    @Mock
    private DcService dcService;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private XpipeMetaManager xpipeMetaManager;

    @Mock
    private RedisCheckRuleServiceImpl redisCheckRuleService;

    @Mock
    private RouteChooseStrategyFactory routeChooseStrategyFactory;

    @InjectMocks
    private DefaultMetaCache metaCache = new DefaultMetaCache();

    @Before
    public void beforeDefaultMetaCacheTest() {
        MockitoAnnotations.initMocks(this);
        metaCache.setMeta(Pair.of(getXpipeMeta(), xpipeMetaManager));
        metaCache.setMonitor2ClusterShard(Maps.newHashMap());
    }

    @Test
    public void leaderTest() throws Exception {
        when(consoleConfig.getCacheRefreshInterval()).thenReturn(10);
        metaCache.isleader();
        Assert.assertTrue(metaCache.getTaskTrigger().get());
        Assert.assertNotNull(metaCache.getFuture());

        metaCache.notLeader();

        Assert.assertFalse(metaCache.getTaskTrigger().get());
        Assert.assertNull(metaCache.getFuture());
        Assert.assertNull(metaCache.meta);
        Assert.assertEquals(metaCache.DEFAULT_KEEPER_NUMBERS, metaCache.allKeeperSize);
        Assert.assertEquals(0, metaCache.lastUpdateTime);
        Assert.assertNull(metaCache.allKeepers);

        metaCache.isleader();
        Assert.assertTrue(metaCache.getTaskTrigger().get());
        Assert.assertNotNull(metaCache.getFuture());
    }


    @Test
    public void getRouteIfPossible() {
        HostPort hostPort = new HostPort("127.0.0.1", 6379);
        XpipeMetaManager xpipeMetaManager = mock(XpipeMetaManager.class);
        when(xpipeMetaManager.findMetaDesc(hostPort)).thenReturn(null);
        metaCache.setMeta(new Pair<>(mock(XpipeMeta.class), xpipeMetaManager));
        metaCache.getCurrentDcConsoleRoutes();
    }

    @Test
    public void testIsCrossRegion() {
        Map<String, DcMeta> dcs = getXpipeMeta().getDcs();
        Assert.assertFalse(dcs.get("jq").getZone().equalsIgnoreCase(dcs.get("fra-aws").getZone()));
    }

    @Test
    public void testGetAllRedisCheckRules() {
        Map<Long, RedisCheckRuleMeta> redisCheckRules = getXpipeMeta().getRedisCheckRules();
        redisCheckRules.values().forEach(redisCheckRuleMeta -> {
            logger.info(redisCheckRuleMeta.getId() + ":" + redisCheckRuleMeta.getCheckType()
                    + ":" + redisCheckRuleMeta.getParam());
        });

        Assert.assertEquals(3, redisCheckRules.values().size());
    }


    @Test
    public void testGetAllActiveRedisOfDc() {
        List<HostPort> redises = metaCache.getAllActiveRedisOfDc("jq", "jq");
        Assert.assertEquals(4, redises.size());
        Assert.assertTrue(redises.contains(new HostPort("10.0.0.1", 6379)));
        Assert.assertTrue(redises.contains(new HostPort("127.0.0.1", 6379)));

        redises = metaCache.getAllActiveRedisOfDc("jq", "oy");
        Assert.assertEquals(2, redises.size());
        Assert.assertTrue(redises.contains(new HostPort("127.0.0.1", 8100)));
        Assert.assertTrue(redises.contains(new HostPort("127.0.0.1", 8101)));

        redises = metaCache.getAllActiveRedisOfDc("oy", "oy");
        Assert.assertEquals(4, redises.size());
        Assert.assertTrue(redises.contains(new HostPort("127.0.0.2", 8100)));
        Assert.assertTrue(redises.contains(new HostPort("127.0.0.2", 8101)));
        Assert.assertTrue(redises.contains(new HostPort("10.0.0.2", 6379)));
        Assert.assertTrue(redises.contains(new HostPort("10.0.0.2", 6479)));
    }

    @Test
    public void testGetAllKeepers() {
        Set<HostPort> allKeepers = metaCache.getAllKeepers();
        Assert.assertEquals(6, allKeepers.size());
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.1", 6000),
                new HostPort("127.0.0.1", 6001),
                new HostPort("127.0.0.1", 6100),
                new HostPort("127.0.0.1", 6101),
                new HostPort("127.0.0.2", 6100),
                new HostPort("127.0.0.2", 6101)), allKeepers);
    }

    @Test
    public void testDivideKeeperAndCluster() {
        when(consoleConfig.getClusterDividedParts()).thenReturn(3);

        metaCache.refreshMetaParts(metaCache.getXpipeMeta());

        Set<HostPort> allKeepers = new HashSet<>();
        Set<Long> allCluster = new HashSet<>();
        XpipeMeta meta = metaCache.getDividedXpipeMeta(0);

        for(DcMeta dcMeta : meta.getDcs().values()) {
            for(KeeperContainerMeta keeperContainerMeta : dcMeta.getKeeperContainers()) {
                allKeepers.add(new HostPort(keeperContainerMeta.getIp(), keeperContainerMeta.getPort()));
            }
            for(ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                allCluster.add(clusterMeta.getDbId());
            }
        }

        Assert.assertEquals(1, allKeepers.size());
        Assert.assertEquals(Sets.newHashSet(new HostPort("1.1.1.3", 8080)), allKeepers);

        Assert.assertEquals(1, allCluster.size());
        Assert.assertEquals(3, allCluster.stream().findFirst().get().intValue());


    }



    @Test
    public void testGetAllKeeperContainersDcMap() {
        Map<String, String> allKeeperContainersDcMap = metaCache.getAllKeeperContainersDcMap();
        Assert.assertEquals("jq", allKeeperContainersDcMap.get("1.1.1.1"));
        Assert.assertEquals("jq", allKeeperContainersDcMap.get("1.1.1.2"));
        Assert.assertEquals("oy", allKeeperContainersDcMap.get("1.1.1.3"));
        Assert.assertEquals("oy", allKeeperContainersDcMap.get("1.1.1.4"));
        Assert.assertEquals(4, allKeeperContainersDcMap.size());
    }

    @Test
    public void testGetAllApplierContainersDcMap() {
        Map<String, String> allKeeperContainersDcMap = metaCache.getAllApplierContainersDcMap();
        Assert.assertEquals("jq", allKeeperContainersDcMap.get("1.1.1.11"));
        Assert.assertEquals("jq", allKeeperContainersDcMap.get("1.1.1.12"));
        Assert.assertEquals("oy", allKeeperContainersDcMap.get("1.1.1.13"));
        Assert.assertEquals("oy", allKeeperContainersDcMap.get("1.1.1.14"));
        Assert.assertEquals(4, allKeeperContainersDcMap.size());
    }


    @Test
    public void testFindBiClusterShardBySentinelMonitor() {
        String monitorNameOY = SentinelUtil.getSentinelMonitorName("cluster3", "shard1", "oy");
        String monitorNameJQ = SentinelUtil.getSentinelMonitorName("cluster3", "shard1", "jq");
        Triple<String, String, Long> clusterAndShardOY = metaCache.findClusterShardBySentinelMonitor(monitorNameOY);
        Assert.assertEquals("cluster3", clusterAndShardOY.getFirst());
        Assert.assertEquals("shard1", clusterAndShardOY.getMiddle());

        Triple<String, String, Long> clusterAndShardJQ = metaCache.findClusterShardBySentinelMonitor(monitorNameJQ);
        Assert.assertEquals("cluster3", clusterAndShardJQ.getFirst());
        Assert.assertEquals("shard1", clusterAndShardJQ.getMiddle());
    }

    @Test
    public void testFindOneWayClusterShardBySentinelMonitor() {
        String monitorNameOY = SentinelUtil.getSentinelMonitorName("cluster1", "shard1", "oy");
        String monitorNameJQ = SentinelUtil.getSentinelMonitorName("cluster1", "shard1", "jq");

        Triple<String, String, Long> clusterAndShardOY = metaCache.findClusterShardBySentinelMonitor(monitorNameOY);
        Assert.assertNull(clusterAndShardOY);

        Triple<String, String, Long> clusterAndShardJQ = metaCache.findClusterShardBySentinelMonitor(monitorNameJQ);
        Assert.assertEquals("cluster1", clusterAndShardJQ.getFirst());
        Assert.assertEquals("shard1", clusterAndShardJQ.getMiddle());
    }

    @Test
    public void getMaxMasterCountDcTest() throws Exception {
        when(consoleConfig.getConsoleAddress()).thenReturn("");
        when(consoleConfig.getClustersPartIndex()).thenReturn(1);
        XpipeMeta xpipeMeta = new XpipeMeta();

//          single dc trocks
        ClusterMeta oyCluster = new ClusterMeta().setType(ClusterType.CROSS_DC.name()).setId("cluster").addShard(
                new ShardMeta().setId("shard1").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));

        DcMeta oyDcMeta = new DcMeta("oy");
        oyDcMeta.addCluster(oyCluster);
        xpipeMeta.addDc(oyDcMeta);
        metaCache.setActiveDcForCrossDcClusters(xpipeMeta);

        Assert.assertEquals("oy", oyCluster.getActiveDc());

//        multi dc trocks，single shard，oy master
        ClusterMeta jqCluster = new ClusterMeta().setType(ClusterType.CROSS_DC.name()).setId("cluster").addShard(
                new ShardMeta().setId("shard1").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));

        DcMeta jqDcMeta = new DcMeta("jq");
        jqDcMeta.addCluster(jqCluster);
        xpipeMeta.addDc(jqDcMeta);
        metaCache.setActiveDcForCrossDcClusters(xpipeMeta);

        Assert.assertEquals("oy", oyCluster.getActiveDc());
        Assert.assertEquals("oy", jqCluster.getActiveDc());

//        multi dc trocks，multi shards，oy master
        oyCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        metaCache.setActiveDcForCrossDcClusters(xpipeMeta);

        Assert.assertEquals("oy", oyCluster.getActiveDc());
        Assert.assertEquals("oy", jqCluster.getActiveDc());

//        multi dc trocks，multi shards，one oy master，one jq master
        oyCluster.removeShard("shard2");
        jqCluster.removeShard("shard2");
        oyCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        metaCache.setActiveDcForCrossDcClusters(xpipeMeta);

        Assert.assertEquals("jq", oyCluster.getActiveDc());
        Assert.assertEquals("jq", jqCluster.getActiveDc());

//        multidc trocks，multi shards，one oy master，two jq masters
        oyCluster.addShard(new ShardMeta().setId("shard3").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard3").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        metaCache.setActiveDcForCrossDcClusters(xpipeMeta);
        Assert.assertEquals("jq", oyCluster.getActiveDc());
        Assert.assertEquals("jq", jqCluster.getActiveDc());
    }

    @Test
    public void checkDcsCntTest() {
        metaCache.setMeta(null);
        try {
            metaCache.checkMeta(new XpipeMeta(), 1, 50);
        } catch (Throwable th) {
            Assert.fail();
        }

        //removed too many dcs
        XpipeMeta currentMeta = new XpipeMeta();
        currentMeta.addDc(new DcMeta("dc1").addCluster(new ClusterMeta("cluster1")))
                .addDc(new DcMeta("dc2").addCluster(new ClusterMeta("cluster1")))
                .addDc(new DcMeta("dc3").addCluster(new ClusterMeta("cluster1")))
                .addDc(new DcMeta("dc4").addCluster(new ClusterMeta("cluster1")));

        XpipeMeta futureMeta = new XpipeMeta();
        futureMeta.addDc(new DcMeta("dc1").addCluster(new ClusterMeta("cluster1")))
                .addDc(new DcMeta("dc2").addCluster(new ClusterMeta("cluster1")));

        Pair<XpipeMeta, XpipeMetaManager> current = new Pair<>(currentMeta, new DefaultXpipeMetaManager(currentMeta));
        metaCache.setMeta(current);
        try {
            metaCache.checkMeta(futureMeta, 1, 50);
            Assert.fail();
        } catch (Throwable th) {
            Assert.assertTrue(th instanceof TooManyDcsRemovedException);
        }
        try {
            metaCache.checkMeta(futureMeta, 2, 50);
        } catch (Throwable th) {
            Assert.fail();
        }

        //add dcs
        current = new Pair<>(futureMeta, new DefaultXpipeMetaManager(futureMeta));
        metaCache.setMeta(current);
        try {
            metaCache.checkMeta(currentMeta, 1, 50);
        } catch (Throwable th) {
            Assert.fail();
        }
    }

    @Test
    public void checkClustersCntTest() {
        //removed too many clusters
        XpipeMeta currentMeta = new XpipeMeta();
        currentMeta.addDc(new DcMeta("dc1").addCluster(new ClusterMeta("cluster1")).addCluster(new ClusterMeta("cluster2")).addCluster(new ClusterMeta("cluster3")))
                .addDc(new DcMeta("dc2").addCluster(new ClusterMeta("cluster1")));


        XpipeMeta futureMeta = new XpipeMeta();
        futureMeta.addDc(new DcMeta("dc1").addCluster(new ClusterMeta("cluster1")))
                .addDc(new DcMeta("dc2").addCluster(new ClusterMeta("cluster1")));

        Pair<XpipeMeta, XpipeMetaManager> current = new Pair<>(currentMeta, new DefaultXpipeMetaManager(currentMeta));
        metaCache.setMeta(current);
        try {
            metaCache.checkMeta(futureMeta, 1, 50);
            Assert.fail();
        } catch (Throwable th) {
            Assert.assertTrue(th instanceof TooManyClustersRemovedException);
        }
        try {
            metaCache.checkMeta(futureMeta, 1, 80);
        } catch (Throwable th) {
            Assert.fail();
        }

        //add clusters
        current = new Pair<>(futureMeta, new DefaultXpipeMetaManager(futureMeta));
        metaCache.setMeta(current);
        try {
            metaCache.checkMeta(currentMeta, 1, 50);
        } catch (Throwable th) {
            Assert.fail();
        }
    }

    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }
}