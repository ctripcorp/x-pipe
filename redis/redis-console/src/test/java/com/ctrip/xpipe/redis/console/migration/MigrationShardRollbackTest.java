package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationShardTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/6/8
 */
public class MigrationShardRollbackTest extends AbstractMigrationTest {

    private MigrationCluster migrationCluster;

    @Mock
    private MigrationCommandBuilder migrationCommandBuilder;

    @Mock
    private MigrationEvent migrationEvent;

    @Autowired
    private ClusterMetaService clusterMetaService;
    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;
    @Autowired
    private AzGroupCache azGroupCache;

    private String dcA = dcNames[0];
    private String dcB = dcNames[1];

    @Override
    public String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-data-miss-test.sql");
    }

    @Before
    public void setupMigrationDataMissTest() {
        MockitoAnnotations.initMocks(this);

        MigrationClusterTbl migrationClusterTbl = migrationService.findMigrationCluster(1L, 1L);
        migrationCluster = new DefaultMigrationCluster(executors, scheduled, migrationEvent, migrationClusterTbl, azGroupClusterRepository, azGroupCache, dcService, clusterService, shardService, redisService, migrationService);

        Map<Long, DcTbl> dcs = new HashMap<>();
        for (DcTbl dc : dcService.findClusterRelatedDc("cluster1")) {
            dcs.put(dc.getId(), dc);
        }

        List<MigrationShardTbl> migrationShards = migrationService.findMigrationShards(1);
        for (MigrationShardTbl migrationShardTbl: migrationShards) {
            MigrationShard migrationShard = new DefaultMigrationShard(migrationCluster, migrationShardTbl,
                    shardService.find(migrationShardTbl.getShardId()), dcs, migrationService, migrationCommandBuilder);
            migrationCluster.addNewMigrationShard(migrationShard);
        }
    }

    @Test
    public void testContinueAfterMigrationShardRollback() throws Exception {
        // test migration shard rollback for multi-console do migration together
        mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard1", dcB, dcA);
        mockSuccessOtherDcCommand(migrationCommandBuilder,"cluster1", "shard2", dcB, dcA);

        migrationCluster.process();
        sleep(1000);

        ClusterTbl currentCluster = clusterService.find(1);
        Assert.assertEquals(MigrationStatus.Success, migrationCluster.getStatus());
        Assert.assertEquals(ClusterStatus.Normal.toString(), currentCluster.getStatus());
        Assert.assertEquals(0L, currentCluster.getMigrationEventId());
        Assert.assertEquals(2, currentCluster.getActivedcId());

        for (MigrationShard migrationShard: migrationCluster.getMigrationShards()) {
            Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
            Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
            Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
            Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));
            Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
        }

        ClusterMeta prevPrimaryDcMeta = clusterMetaService.getClusterMeta(dcA, "cluster1");
        Assert.assertEquals(dcB, prevPrimaryDcMeta.getActiveDc());
        ClusterMeta newPrimaryDcMeta = clusterMetaService.getClusterMeta(dcB, "cluster1");
        Assert.assertEquals(dcB, newPrimaryDcMeta.getActiveDc());
    }



}
