package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.support.HeteroMigrationSupport;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Map;

public class DefaultMigrationClusterHeteroTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;

    @Autowired
    private AzGroupCache azGroupCache;

    @Autowired
    private MigrationService migrationService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private HeteroMigrationSupport heteroMigrationSupport;

    @Test
    public void heteroUpdateActiveDcShouldOnlyTouchTargetAzGroup() {
        ClusterTbl clusterTbl = clusterService.find("hetero-dual-oneway");
        Assert.assertEquals(1L, clusterTbl.getActivedcId());

        AzGroupClusterEntity shaOneWay = azGroupClusterRepository.selectByClusterId(clusterTbl.getId()).stream()
                .filter(entity -> entity.getAzGroupId() == 1L)
                .findFirst()
                .orElse(null);
        AzGroupClusterEntity sgpOneWay = azGroupClusterRepository.selectByClusterId(clusterTbl.getId()).stream()
                .filter(entity -> entity.getAzGroupId() == 2L)
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(shaOneWay);
        Assert.assertNotNull(sgpOneWay);
        Assert.assertEquals(1L, shaOneWay.getActiveAzId().longValue());
        Assert.assertEquals(3L, sgpOneWay.getActiveAzId().longValue());

        MigrationClusterTbl migrationClusterTbl = new MigrationClusterTbl()
                .setClusterId(clusterTbl.getId())
                .setSourceDcId(1L)
                .setDestinationDcId(2L)
                .setStatus("Initiated");
        DefaultMigrationCluster migrationCluster = new DefaultMigrationCluster(executors, scheduled,
                Mockito.mock(MigrationEvent.class), migrationClusterTbl, azGroupClusterRepository, azGroupCache,
                heteroMigrationSupport, dcService, clusterService, shardService, redisService, migrationService);

        Map<Long, com.ctrip.xpipe.redis.console.model.DcTbl> clusterDcs = migrationCluster.getClusterDcs();
        Assert.assertEquals(2, clusterDcs.size());
        Assert.assertTrue(clusterDcs.containsKey(1L));
        Assert.assertTrue(clusterDcs.containsKey(2L));
        Assert.assertFalse(clusterDcs.containsKey(3L));

        migrationCluster.updateActiveDcIdToDestDcId();

        ClusterTbl updatedCluster = clusterService.find(clusterTbl.getId());
        Assert.assertEquals(1L, updatedCluster.getActivedcId());

        shaOneWay = azGroupClusterRepository.selectById(shaOneWay.getId());
        sgpOneWay = azGroupClusterRepository.selectById(sgpOneWay.getId());
        Assert.assertEquals(2L, shaOneWay.getActiveAzId().longValue());
        Assert.assertEquals(3L, sgpOneWay.getActiveAzId().longValue());
        Assert.assertEquals(ClusterType.ONE_WAY.toString(), shaOneWay.getAzGroupClusterType());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }
}
