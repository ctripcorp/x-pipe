package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.SourceMeta;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author chen.zhu
 * <p>
 * Apr 03, 2018
 */
public class DcMetaBuilderTest extends AbstractConsoleIntegrationTest {

    private Map<String, DcMeta> dcMetaMap = new HashMap<>();

    private DcMeta dcMeta = new DcMeta();

    private Map<Long, String> dcNameMap;

    private long dcId;

    @Autowired
    private RedisMetaService redisMetaService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private ClusterMetaService clusterMetaService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private MigrationService migrationService;

    @Autowired
    private ReplDirectionService replDirectionService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ZoneService zoneService;

    @Autowired
    private KeeperContainerService keeperContainerService;

    @Autowired
    private ApplierService applierService;

    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;

    @Autowired
    private AzGroupCache azGroupCache;

    private List<DcClusterShardTbl> dcClusterShards;

    private DcMetaBuilder builder;

    @Before
    public void beforeDcMetaBuilderTest() throws Exception {
        dcNameMap = dcService.dcNameMap();
        dcId = dcNameMap.keySet().iterator().next();
        List<DcTbl> dcTblList = dcService.findAllDcs();
        builder = new DcMetaBuilder(dcMetaMap, dcTblList, Collections.singleton(ClusterType.ONE_WAY.toString()),
            executors, redisMetaService, dcClusterService, clusterMetaService, dcClusterShardService, dcService,
            azGroupClusterRepository, azGroupCache, replDirectionService, zoneService, keeperContainerService,
            applierService, new DefaultRetryCommandFactory(), consoleConfig);
        builder.execute().get();

        logger.info("[beforeDcMetaBuilderTest] dcId: {}", dcId);
        dcClusterShards = dcClusterShardService.findAllByDcId(dcId);
        logger.info("[beforeDcMetaBuilderTest] dcClusterShards: {}", dcClusterShards);
    }

    @Test
    public void testBuildMetaForClusterType() throws Exception {
        testBuildMetaForClusterType(ClusterType.ONE_WAY, 6);
        testBuildMetaForClusterType(ClusterType.BI_DIRECTION, 1);
    }

    private void tryCreateClusterMeta(ClusterTbl clusterTbl, DcClusterTbl dcClusterTbl) {
        try {
            builder.getOrCreateClusterMeta(dcMeta, dcId, clusterTbl, dcClusterTbl, null);
        } catch (Exception e) {
            logger.info("[tryCreateClusterMeta] create fail", e);
            throw e;
        }
    }

    @Test
    public void getOrCreateOneWayClusterMeta() throws Exception {
        tryCreateClusterMeta(dcClusterShards.get(0).getClusterInfo(), dcClusterShards.get(0).getDcClusterInfo());
        ClusterMeta clusterMeta = dcMeta.getClusters().get(dcClusterShards.get(0).getClusterInfo().getClusterName());
        Assert.assertNotNull(clusterMeta);
        Assert.assertTrue(ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.ONE_WAY));
        Assert.assertEquals("jq", clusterMeta.getActiveDc());
        Assert.assertEquals("oy,fra", clusterMeta.getBackupDcs());
        Assert.assertEquals("1,2", clusterMeta.getClusterDesignatedRouteIds());
        Assert.assertNull(clusterMeta.getDcs());
    }

    @Test
    public void getOrCreateBiDirectionClusterMeta() throws Exception {
        ClusterTbl clusterTbl = clusterService.find("bi-cluster1");
        Assert.assertNotNull(clusterTbl);

        tryCreateClusterMeta(clusterTbl, dcClusterService.find(1, clusterTbl.getId()));
        ClusterMeta clusterMeta = dcMeta.getClusters().get("bi-cluster1");
        Assert.assertNotNull(clusterMeta);
        Assert.assertTrue(ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.BI_DIRECTION));
        Assert.assertNull(clusterMeta.getActiveDc());
        Assert.assertNull(clusterMeta.getBackupDcs());
        Assert.assertEquals("jq,oy", clusterMeta.getDcs());
        Assert.assertEquals("1,2",clusterMeta.getActiveRedisCheckRules());
        Assert.assertEquals("1,2",clusterMeta.getClusterDesignatedRouteIds());
    }

    @Test
    public void getOrCreateHeteroClusterMeta() throws Exception {
        ClusterTbl clusterTbl = clusterService.find("asymmetric2-local-cluster");
        Assert.assertNotNull(clusterTbl);

        tryCreateClusterMeta(clusterTbl, dcClusterService.find(1, clusterTbl.getId()));
        ClusterMeta clusterMeta = dcMeta.getClusters().get("asymmetric2-local-cluster");
        Assert.assertNotNull(clusterMeta);

        Assert.assertTrue(ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.ONE_WAY));
        Assert.assertEquals("jq", clusterMeta.getActiveDc());
        Assert.assertEquals("", clusterMeta.getBackupDcs());
    }

    @Test
    public void getOrCreateShardMeta() throws Exception {
        DcClusterShardTbl dcClusterShard = dcClusterShards.get(0);
        Assert.assertNotNull(dcClusterShard);
        ClusterTbl clusterTbl = dcClusterShard.getClusterInfo();
        Assert.assertNotNull(clusterTbl);
        tryCreateClusterMeta(clusterTbl, dcClusterShard.getDcClusterInfo());
        ClusterMeta clusterMeta = dcMeta.getClusters().get(dcClusterShard.getClusterInfo().getClusterName());

        logger.info("{}", dcClusterShard.getShardInfo());

        builder.getOrCreateShardMeta(dcMeta, clusterMeta.getId(), dcClusterShard.getShardInfo(), dcClusterShard.getSetinelId());


        String clusterId = dcClusterShard.getClusterInfo().getClusterName(), shardId = dcClusterShard.getShardInfo().getShardName();

        ShardMeta shardMeta = dcMeta.findCluster(clusterId).findShard(shardId);
        Assert.assertNotNull(shardMeta);
        logger.info("{}", shardMeta);
    }


    @Test
    public void testClusterBondToOnlyOneIDC() throws Exception {

        ClusterModel clusterModel = new ClusterModel();
        ClusterTbl clusterTbl = new ClusterTbl();
        clusterTbl.setActivedcId(1).setClusterAdminEmails("test@ctrip.com").setClusterName("test-one-dc-cluster")
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterOrgId(1).setClusterDescription("not null").setStatus("Normal");
        clusterModel.setClusterTbl(clusterTbl);
        clusterModel.setShards(Lists.newArrayList());
        //empty slave idc
        clusterModel.setDcs(Lists.newArrayList());
        clusterTbl = clusterService.createCluster(clusterModel);

//        dcClusterService.addDcCluster(dcService.getDcName(1), "test-one-dc-cluster");

        Map<Long, List<DcClusterTbl>> map = Maps.newHashMap();
        map.put(clusterTbl.getId(), Lists.newArrayList(new DcClusterTbl().setClusterId(clusterTbl.getId()).setDcId(1)));
        builder.setCluster2DcClusterMap(map);
    }

    @Test
    public void testHeteroPrimaryDc() throws ExecutionException, InterruptedException {
        DcMeta dcMeta = new DcMeta();
        dcMetaMap.clear();
        dcMetaMap.put("JQ", dcMeta);
        List<DcTbl> dcTblList = dcService.findAllDcs();

        new DcMetaBuilder(dcMetaMap, dcTblList, Collections.singleton(ClusterType.ONE_WAY.name()), executors,
            redisMetaService, dcClusterService, clusterMetaService, dcClusterShardService, dcService,
            azGroupClusterRepository, azGroupCache, replDirectionService, zoneService, keeperContainerService,
            applierService, new DefaultRetryCommandFactory(), consoleConfig).execute().get();

        boolean found = false;
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            if (clusterMeta.getId().equals("asymmetric2-local-cluster")) {
                found = true;
            } else {
                continue;
            }

            Assert.assertEquals(ClusterType.ONE_WAY.name(), clusterMeta.getType().toUpperCase());
            Assert.assertEquals("jq", clusterMeta.getActiveDc());
            Assert.assertEquals("", clusterMeta.getBackupDcs());
            Assert.assertEquals("oy", clusterMeta.getDownstreamDcs());
            Assert.assertEquals("ONE_WAY", clusterMeta.getAzGroupType());
            Assert.assertEquals("LOCAL_JQ", clusterMeta.getAzGroupName());
            Collection<ShardMeta> shardMetas = clusterMeta.getShards().values();
            Assert.assertEquals(2, shardMetas.size());
            for (ShardMeta shardMeta : shardMetas) {
                Assert.assertEquals(2, shardMeta.getRedises().size());
                Assert.assertEquals(2, shardMeta.getKeepers().size());
                Assert.assertEquals(0, shardMeta.getAppliers().size());
            }
            List<SourceMeta> sourceMetas = clusterMeta.getSources();
            Assert.assertEquals(0, sourceMetas.size());
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testGetDcClusterInfoWhenClusterNotExist() {

        List<DcTbl> dcTblList = dcService.findAllDcs();

        DcMetaBuilder dcMetaBuilder = new DcMetaBuilder(dcMetaMap, dcTblList,
            Collections.singleton(ClusterType.ONE_WAY.name()), executors, redisMetaService, dcClusterService,
            clusterMetaService, dcClusterShardService, dcService, azGroupClusterRepository, azGroupCache,
            replDirectionService, zoneService, keeperContainerService, applierService, new DefaultRetryCommandFactory(),
            consoleConfig);

        dcMetaBuilder.cluster2DcClusterMap = new HashMap<>();

        DcMetaBuilder.BuildDcMetaCommand buildDcMetaCommand = dcMetaBuilder.createBuildDcMetaCommand();
        Assert.assertNull(buildDcMetaCommand.getDcClusterInfo(1L, 1L));
    }

    @Test
    public void testHeteroDownstreamDc() throws ExecutionException, InterruptedException {
        DcMeta dcMeta = new DcMeta();
        dcMetaMap.clear();
        dcMetaMap.put("OY", dcMeta);
        List<DcTbl> dcTblList = dcService.findAllDcs();

        new DcMetaBuilder(dcMetaMap, dcTblList, Collections.singleton(ClusterType.ONE_WAY.name()), executors,
            redisMetaService, dcClusterService, clusterMetaService, dcClusterShardService, dcService,
            azGroupClusterRepository, azGroupCache, replDirectionService, zoneService, keeperContainerService,
            applierService, new DefaultRetryCommandFactory(), consoleConfig).execute().get();

        boolean found = false;
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            if (clusterMeta.getId().equals("asymmetric2-local-cluster")) {
                found = true;
            } else {
                continue;
            }
            Assert.assertEquals(ClusterType.ONE_WAY.name(), clusterMeta.getType().toUpperCase());
            Assert.assertEquals("jq", clusterMeta.getActiveDc());
            Assert.assertEquals("", clusterMeta.getBackupDcs());
            Assert.assertEquals("", clusterMeta.getDownstreamDcs());
            Assert.assertEquals("SINGLE_DC", clusterMeta.getAzGroupType());
            Assert.assertEquals("LOCAL_OY", clusterMeta.getAzGroupName());
            Collection<ShardMeta> shardMetas = clusterMeta.getShards().values();
            Assert.assertEquals(1, shardMetas.size());
            for (ShardMeta shardMeta : shardMetas) {
                Assert.assertEquals(2, shardMeta.getRedises().size());
                Assert.assertEquals(0, shardMeta.getAppliers().size());
            }
            List<SourceMeta> sourceMetas = clusterMeta.getSources();
            Assert.assertEquals(1, sourceMetas.size());
            for (ShardMeta shardMeta : sourceMetas.get(0).getShards().values()) {
                Assert.assertEquals(0, shardMeta.getRedises().size());
                Assert.assertEquals(2, shardMeta.getKeepers().size());
                Assert.assertEquals(2, shardMeta.getAppliers().size());
            }
        }
        Assert.assertTrue(found);
    }

    private void testBuildMetaForClusterType(ClusterType clusterType, int clusterSize) throws Exception {
        DcMeta dcMeta = new DcMeta();
        dcMetaMap.clear();
        long dcId = dcNameMap.keySet().iterator().next();
        dcMetaMap.put(dcNameMap.get(dcId).toUpperCase(), dcMeta);

        List<DcTbl> dcTblList = dcService.findAllDcs();

        new DcMetaBuilder(dcMetaMap, dcTblList, Collections.singleton(clusterType.toString()), executors,
            redisMetaService, dcClusterService, clusterMetaService, dcClusterShardService, dcService,
            azGroupClusterRepository, azGroupCache, replDirectionService, zoneService, keeperContainerService,
            applierService, new DefaultRetryCommandFactory(), consoleConfig).execute().get();

        Assert.assertEquals(clusterSize, dcMeta.getClusters().size());
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            Assert.assertTrue(ClusterType.isSameClusterType(clusterMeta.getType(), clusterType));
            if(ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.BI_DIRECTION))
                Assert.assertEquals("1,2", clusterMeta.getActiveRedisCheckRules());
        }
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

}