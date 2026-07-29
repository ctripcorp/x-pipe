package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.cache.impl.AzGroupCacheImpl;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ZoneService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.ClusterMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationServiceImpl;
import com.ctrip.xpipe.redis.console.service.migration.support.HeteroMigrationSupport;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DcMetaBuilderCrossRegionAzGroupTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;

    @Autowired
    private ClusterMetaService clusterMetaService;

    @Autowired
    private ClusterMetaServiceImpl clusterMetaServiceImpl;

    @Autowired
    private MigrationServiceImpl migrationService;

    @Autowired
    private DcService dcService;

    @Autowired
    private ZoneService zoneService;

    @Autowired
    private RedisMetaService redisMetaService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private AzGroupCache azGroupCache;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private HeteroMigrationSupport heteroMigrationSupport;

    @Autowired
    private DcMetaService dcMetaService;

    private Map<Long, String> dcNameMap;

    @Before
    public void prepare() throws Exception {
        clusterMetaServiceImpl.setMigrationService(migrationService);
        dcNameMap = dcService.dcNameMap();
        refreshAzGroupCache();
    }

    @Test
    public void crossRegionOneWayShouldExposeCrossRegionBackupDcs() {
        ClusterTbl clusterTbl = clusterService.find("hetero-cross-region");
        Assert.assertNotNull(clusterTbl);

        AzGroupClusterEntity crossOneWay = azGroupClusterRepository.selectByClusterId(clusterTbl.getId()).stream()
                .filter(entity -> ClusterType.isSameClusterType(entity.getAzGroupClusterType(), ClusterType.ONE_WAY))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(crossOneWay);
        Assert.assertEquals("SHA", heteroMigrationSupport.activeRegion(crossOneWay));
        Set<String> contained = heteroMigrationSupport.containedRegions(crossOneWay);
        Assert.assertTrue(contained.contains("SHA"));
        Assert.assertTrue(contained.contains("FRA"));

        DcMetaBuilder builder = new DcMetaBuilder(new HashMap<>(), dcService.findAllDcs(),
                Collections.singleton(ClusterType.HETERO.toString()), executors, redisMetaService, dcClusterService,
                clusterMetaService, dcClusterShardService, dcService, azGroupClusterRepository, azGroupCache,
                new DefaultRetryCommandFactory(), consoleConfig);
        builder.setDcNameMap(dcNameMap);

        DcClusterTbl jqDcCluster = dcClusterService.find(1, clusterTbl.getId());
        DcClusterTbl oyDcCluster = dcClusterService.find(2, clusterTbl.getId());
        DcClusterTbl fraDcCluster = dcClusterService.find(3, clusterTbl.getId());

        ClusterMeta jqMeta = builder.getOrCreateClusterMeta(new DcMeta("jq"), 1L, clusterTbl, jqDcCluster, crossOneWay);
        ClusterMeta oyMeta = builder.getOrCreateClusterMeta(new DcMeta("oy"), 2L, clusterTbl, oyDcCluster, crossOneWay);
        ClusterMeta fraMeta = builder.getOrCreateClusterMeta(new DcMeta("fra"), 3L, clusterTbl, fraDcCluster, crossOneWay);

        Assert.assertEquals("jq", jqMeta.getActiveDc());
        Assert.assertEquals("jq", oyMeta.getActiveDc());
        Assert.assertEquals("jq", fraMeta.getActiveDc());

        Set<String> jqBackups = new HashSet<>(Arrays.asList(jqMeta.getBackupDcs().split(",")));
        Assert.assertTrue(jqBackups.contains("oy"));
        Assert.assertTrue(jqBackups.contains("fra"));
        Assert.assertFalse(jqBackups.contains("sgp"));

        Map<Long, String> zoneNameMap = zoneService.zoneNameMap();
        DcTbl jq = dcService.find("jq");
        DcTbl fra = dcService.find("fra");
        Assert.assertNotEquals(zoneNameMap.get(jq.getZoneId()), zoneNameMap.get(fra.getZoneId()));
    }

    @Test
    public void findByActiveRegionShouldMatchActiveAzRegionOnly() {
        ClusterTbl clusterTbl = clusterService.find("hetero-cross-region");
        Assert.assertNotNull(clusterTbl);

        AzGroupClusterEntity shaGroup = heteroMigrationSupport.findByActiveRegion(clusterTbl.getId(), "SHA");
        Assert.assertNotNull(shaGroup);
        Assert.assertTrue(ClusterType.isSameClusterType(shaGroup.getAzGroupClusterType(), ClusterType.ONE_WAY));

        AzGroupClusterEntity sgpGroup = heteroMigrationSupport.findByActiveRegion(clusterTbl.getId(), "SGP");
        Assert.assertNotNull(sgpGroup);
        Assert.assertTrue(ClusterType.isSameClusterType(sgpGroup.getAzGroupClusterType(), ClusterType.SINGLE_DC));

        Assert.assertNull(heteroMigrationSupport.findByActiveRegion(clusterTbl.getId(), "FRA"));
    }

    @Test
    public void getDcMetaShouldKeepCrossRegionBackupsForOneWayFilter() throws Exception {
        Set<String> types = new HashSet<>();
        types.add(ClusterType.HETERO.toString());
        types.add(ClusterType.ONE_WAY.toString());
        types.add(ClusterType.SINGLE_DC.toString());

        DcMeta jqMeta = dcMetaService.getDcMeta("jq", types);
        ClusterMeta jqCluster = jqMeta.getClusters().get("hetero-cross-region");
        Assert.assertNotNull(jqCluster);
        Assert.assertEquals(ClusterType.ONE_WAY.toString(), jqCluster.getType());
        Assert.assertEquals("jq", jqCluster.getActiveDc());
        Set<String> backups = new HashSet<>(Arrays.asList(jqCluster.getBackupDcs().split(",")));
        Assert.assertTrue(backups.contains("oy"));
        Assert.assertTrue(backups.contains("fra"));

        DcMeta sgpMeta = dcMetaService.getDcMeta("sgp", types);
        ClusterMeta sgpCluster = sgpMeta.getClusters().get("hetero-cross-region");
        Assert.assertNotNull(sgpCluster);
        Assert.assertEquals(ClusterType.SINGLE_DC.toString(), sgpCluster.getType());
        Assert.assertEquals("sgp", sgpCluster.getActiveDc());
    }

    private void refreshAzGroupCache() throws Exception {
        if (!(azGroupCache instanceof AzGroupCacheImpl)) {
            return;
        }
        Field models = AzGroupCacheImpl.class.getDeclaredField("azGroupModels");
        models.setAccessible(true);
        models.set(azGroupCache, null);
        Field idMap = AzGroupCacheImpl.class.getDeclaredField("idAzGroupMap");
        idMap.setAccessible(true);
        idMap.set(azGroupCache, null);
        azGroupCache.getAllAzGroup();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/hetero-cross-region-az-group-test.sql");
    }
}
