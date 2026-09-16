package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
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

import static org.mockito.Mockito.*;

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

    @Mock
    private ConsoleServiceManager consoleServiceManager;

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
        Assert.assertTrue(metaCache.getIsLeader().get());
        Assert.assertNotNull(metaCache.getFuture());

        metaCache.notLeader();

        Assert.assertFalse(metaCache.getIsLeader().get());
        Assert.assertNull(metaCache.getFuture());
        Assert.assertNull(metaCache.meta);
        Assert.assertEquals(metaCache.DEFAULT_KEEPER_NUMBERS, metaCache.allKeeperSize);
        Assert.assertEquals(0, metaCache.lastUpdateTime);
        Assert.assertNull(metaCache.allKeepers);

        metaCache.isleader();
        Assert.assertTrue(metaCache.getIsLeader().get());
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

        // D47: keeperContainer 不参与分片，每个 part 都带本 DC 全量 container
        Assert.assertEquals(4, allKeepers.size());
        Assert.assertEquals(Sets.newHashSet(new HostPort("1.1.1.1", 8080),
                new HostPort("1.1.1.2", 8080),
                new HostPort("1.1.1.3", 8080),
                new HostPort("1.1.1.4", 8080)), allKeepers);

        Assert.assertEquals(1, allCluster.size());
        Assert.assertEquals(3, allCluster.stream().findFirst().get().intValue());


    }

    /**
     * D47 / AC-19d：container 分片键与 cluster 分片键互相独立时，任一 part 的 keeper 都必须能在同 DcMeta 内
     * 解析出自己的 KeeperContainer —— 这正是 Checker 构建 {@code instance.isTfs} 的点查路径。
     */
    @Test
    public void testDividedMetaCarriesAllKeeperContainersForEveryPart() {
        int parts = 2;
        XpipeMeta full = keeperContainerCrossPartMeta();
        Set<Long> allContainerIds = Sets.newHashSet(CONTAINER_ID_OF_PART_0, CONTAINER_ID_OF_PART_1);

        metaCache.setMeta(Pair.of(full, xpipeMetaManager));
        when(consoleConfig.getClusterDividedParts()).thenReturn(parts);
        metaCache.refreshMetaParts(full);

        for (int partIndex = 0; partIndex < parts; partIndex++) {
            XpipeMeta part = metaCache.getDividedXpipeMeta(partIndex);
            Assert.assertEquals(full.getDcs().keySet(), part.getDcs().keySet());

            for (DcMeta dcMeta : part.getDcs().values()) {
                Map<Long, String> diskTypeByContainerId = Maps.newHashMap();
                dcMeta.getKeeperContainers().forEach(container ->
                        diskTypeByContainerId.put(container.getId(), container.getDiskType()));
                Assert.assertEquals(allContainerIds, diskTypeByContainerId.keySet());

                for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                    Assert.assertEquals(partIndex, (int) (clusterMeta.getDbId() % parts));
                    for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                        for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                            Assert.assertEquals("TFS", diskTypeByContainerId.get(keeperMeta.getKeeperContainerId()));
                        }
                    }
                }
            }
        }
    }

    private static final long CONTAINER_ID_OF_PART_0 = 10L;

    private static final long CONTAINER_ID_OF_PART_1 = 11L;

    /**
     * 单 DC 两 cluster：cluster dbId 与其 keeper 所属 containerId 落在不同 part（parts = 2）。
     */
    private XpipeMeta keeperContainerCrossPartMeta() {
        DcMeta dcMeta = new DcMeta().setId("jq").setZone("SHA");
        dcMeta.addKeeperContainer(new KeeperContainerMeta().setId(CONTAINER_ID_OF_PART_0)
                .setIp("1.1.1.10").setPort(8080).setDiskType("TFS"));
        dcMeta.addKeeperContainer(new KeeperContainerMeta().setId(CONTAINER_ID_OF_PART_1)
                .setIp("1.1.1.11").setPort(8080).setDiskType("TFS"));

        dcMeta.addCluster(oneWayClusterWithKeeper("clusterInPart1", 1L, CONTAINER_ID_OF_PART_0, 6000));
        dcMeta.addCluster(oneWayClusterWithKeeper("clusterInPart0", 2L, CONTAINER_ID_OF_PART_1, 6100));

        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(dcMeta);
        return xpipeMeta;
    }

    private ClusterMeta oneWayClusterWithKeeper(String clusterId, long clusterDbId, long keeperContainerId, int keeperPort) {
        ClusterMeta clusterMeta = new ClusterMeta().setId(clusterId).setDbId(clusterDbId)
                .setType(ClusterType.ONE_WAY.toString()).setActiveDc("jq");
        ShardMeta shardMeta = new ShardMeta().setId("shard").setDbId(clusterDbId);
        shardMeta.addKeeper(new KeeperMeta().setIp("127.0.0.1").setPort(keeperPort).setActive(true)
                .setKeeperContainerId(keeperContainerId));
        clusterMeta.addShard(shardMeta);
        return clusterMeta;
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
        currentMeta.addDc(new DcMeta("dc1").addCluster(new ClusterMeta("cluster1")).addCluster(new ClusterMeta("cluster2")).addCluster(new ClusterMeta("cluster3")).addCluster(new ClusterMeta("cluster4")).addCluster(new ClusterMeta("cluster5")).addCluster(new ClusterMeta("cluster6")).addCluster(new ClusterMeta("cluster7")))
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
            metaCache.checkMeta(futureMeta, 1, 90);
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

    @Test
    public void testClusterDnCnt() {
        Map<String, Integer> cntMap = metaCache.getClusterCntMap("cluster1");
        Assert.assertEquals(cntMap.size(), 2);
        Assert.assertEquals(cntMap.get("jq").intValue(), 3);
        Assert.assertEquals(cntMap.get("oy").intValue(), 2);
    }

    @Test
    public void testGetMigratableClustersCountByActiveDc() {
        Assert.assertEquals(1, metaCache.getMigratableClustersCountByActiveDc("jq"));
        Assert.assertEquals(2, metaCache.getMigratableClustersCountByActiveDc("oy"));
        Assert.assertEquals(0, metaCache.getMigratableClustersCountByActiveDc("fra-aws"));
    }

    @Test
    public void testGetAllDcMigratableClustersCnt() {
        Map<String, Integer> allDcClusters = metaCache.getAllDcMigratableClustersCnt();
        Assert.assertEquals(3, allDcClusters.size());
        Assert.assertEquals(1, allDcClusters.get("jq").intValue());
        Assert.assertEquals(2, allDcClusters.get("oy").intValue());
        Assert.assertEquals(0, allDcClusters.get("fra-aws").intValue());
    }

    @Test
    public void testGetCurrentRegionDcs() throws InterruptedException {
        when(consoleConfig.getCacheRefreshInterval()).thenReturn(10);
        when(consoleConfig.getRegionDcsRefreshIntervalMilli()).thenReturn(100L);
        metaCache.isleader();

        List<String> regionDcs = null;
        // is leader, no cache ,no meta
        try {
            regionDcs = metaCache.currentRegionDcs();
            Assert.fail();
        } catch (Throwable th) {
            Assert.assertNull(regionDcs);
            Assert.assertTrue(th instanceof DataNotFoundException);
        }

        // meta not null
        metaCache.setMeta(Pair.of(getXpipeMeta(), xpipeMetaManager));
        metaCache.setMonitor2ClusterShard(Maps.newHashMap());

        regionDcs = metaCache.currentRegionDcs();
        Assert.assertEquals(2, regionDcs.size());
        Assert.assertTrue(regionDcs.contains("jq"));
        Assert.assertTrue(regionDcs.contains("oy"));
        verify(consoleServiceManager, never()).dcsInCurrentRegion();

        //meta is null, cache expired
        Thread.sleep(150);
        metaCache.notLeader();
        metaCache.isleader();
        regionDcs = metaCache.currentRegionDcs();
        Assert.assertEquals(2, regionDcs.size());
        Assert.assertTrue(regionDcs.contains("jq"));
        Assert.assertTrue(regionDcs.contains("oy"));
        verify(consoleServiceManager, never()).dcsInCurrentRegion();

        //not leader, use cache
        metaCache.notLeader();
        doThrow(new DataNotFoundException("data not ready")).when(consoleServiceManager).dcsInCurrentRegion();
        regionDcs = metaCache.currentRegionDcs();
        Assert.assertEquals(2, regionDcs.size());
        Assert.assertTrue(regionDcs.contains("jq"));
        Assert.assertTrue(regionDcs.contains("oy"));
        verify(consoleServiceManager, times(1)).dcsInCurrentRegion();

    }

    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }
}