package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.ClusterMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationServiceImpl;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DcMetaBuilderHeteroMigrationTest extends AbstractConsoleIntegrationTest {

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
    private RedisMetaService redisMetaService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private AzGroupCache azGroupCache;

    @Autowired
    private ConsoleConfig consoleConfig;

    private Map<Long, String> dcNameMap;

    @Before
    public void prepare() {
        clusterMetaServiceImpl.setMigrationService(migrationService);
        dcNameMap = dcService.dcNameMap();
    }

    @Test
    public void heteroOneWayShouldExposeDestinationAsActiveDuringMigration() {
        ClusterTbl clusterTbl = clusterService.find("hetero-dual-oneway");
        Assert.assertNotNull(clusterTbl);

        AzGroupClusterEntity shaOneWayAzGroup = azGroupClusterRepository.selectByClusterId(clusterTbl.getId()).stream()
                .filter(entity -> ClusterType.isSameClusterType(entity.getAzGroupClusterType(), ClusterType.ONE_WAY)
                        && entity.getAzGroupId() == 1L)
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(shaOneWayAzGroup);

        DcMetaBuilder builder = new DcMetaBuilder(new HashMap<>(), dcService.findAllDcs(),
                Collections.singleton(ClusterType.HETERO.toString()), executors, redisMetaService, dcClusterService,
                clusterMetaService, dcClusterShardService, dcService, azGroupClusterRepository, azGroupCache,
                new DefaultRetryCommandFactory(), consoleConfig);
        builder.setDcNameMap(dcNameMap);

        DcClusterTbl jqDcCluster = dcClusterService.find(1, clusterTbl.getId());
        DcClusterTbl oyDcCluster = dcClusterService.find(2, clusterTbl.getId());
        DcClusterTbl fraDcCluster = dcClusterService.find(3, clusterTbl.getId());

        ClusterMeta jqMeta = builder.getOrCreateClusterMeta(new DcMeta("jq"), 1L, clusterTbl, jqDcCluster, shaOneWayAzGroup);
        ClusterMeta oyMeta = builder.getOrCreateClusterMeta(new DcMeta("oy"), 2L, clusterTbl, oyDcCluster, shaOneWayAzGroup);

        AzGroupClusterEntity fraOneWayAzGroup = azGroupClusterRepository.selectByClusterId(clusterTbl.getId()).stream()
                .filter(entity -> ClusterType.isSameClusterType(entity.getAzGroupClusterType(), ClusterType.ONE_WAY)
                        && entity.getAzGroupId() == 2L)
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(fraOneWayAzGroup);
        ClusterMeta fraMeta = builder.getOrCreateClusterMeta(new DcMeta("fra"), 3L, clusterTbl, fraDcCluster, fraOneWayAzGroup);

        Assert.assertEquals("jq", jqMeta.getActiveDc());
        Assert.assertEquals("oy", oyMeta.getActiveDc());
        Assert.assertEquals("fra", fraMeta.getActiveDc());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql")
                + "update CLUSTER_TBL set status='Migrating', migration_event_id=200 where id=14;\n"
                + "insert into MIGRATION_EVENT_TBL (id, event_tag) values (200, 'hetero-dual-oneway-jq-oy');\n"
                + "insert into MIGRATION_CLUSTER_TBL (id, migration_event_id, cluster_id, source_dc_id, destination_dc_id, status) "
                + "values (200, 200, 14, 1, 2, 'Processing');\n";
    }
}
