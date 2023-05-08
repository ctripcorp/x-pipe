package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.model.*;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultShardMigrationResult;
import com.ctrip.xpipe.redis.console.model.MigrationClusterInfo;
import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MigrationEventDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationEventDao migrationEventDao;

    @Override
    public String prepareDatas() {
        try {
            return prepareDatasFromFile("src/test/resources/migration-test.sql");
        } catch (Exception ex) {
            logger.error("Prepare data from file failed", ex);
        }
        return "";
    }

    @Test
    public void testGetMigrationCluster(){

        List<MigrationClusterModel> migrationCluster = migrationEventDao.getMigrationCluster(2);
        Assert.assertEquals(1, migrationCluster.size());

        MigrationClusterInfo clusterInfo = migrationCluster.iterator().next().getMigrationCluster();

        logger.info("{}", clusterInfo);
        Assert.assertEquals("cluster2", clusterInfo.getClusterName());
        Assert.assertEquals(dcNames[0], clusterInfo.getSourceDcName());
        Assert.assertEquals(dcNames[1], clusterInfo.getDestinationDcName());
        Assert.assertEquals("Checking", clusterInfo.getStatus());
        Assert.assertEquals(2, clusterInfo.getClusterId());
        Assert.assertEquals(2, clusterInfo.getMigrationEventId());
        Assert.assertEquals("", clusterInfo.getPublishInfo());

    }

    @Test
    public void findUnfinishedEvents(){

        List<Long> allUnfinished = migrationEventDao.findAllUnfinished();

        logger.info("{}", allUnfinished.size());
        Set<Long> all = new HashSet<>();

        allUnfinished.forEach(id -> {
                Assert.assertTrue(all.add(id));
            }
        );
    }

    @Test
    public void testGetLatestMigrationOperators(){

        List<MigrationEventTbl> latestMigrateEvent = migrationEventDao.findLatestMigrateEvent(DateTimeUtils.getHoursBeforeDate(new Date(), 1));
        Assert.assertEquals(7, latestMigrateEvent.size());
    }

    @Test
    @DirtiesContext
    public void testBuildMigrationEvent() {

        MigrationEvent event = migrationEventDao.buildMigrationEvent(2);
        Assert.assertNotNull(event);
        Assert.assertEquals(2, event.getMigrationCluster(2).getMigrationShards().size());
    }

    @Test
    @DirtiesContext
    public void testShardResult() {

        MigrationEvent event = migrationEventDao.buildMigrationEvent(2);

        // update check
        MigrationCluster migrationCluster = event.getMigrationClusters().get(0);
        List<MigrationShard> migrationShards = migrationCluster.getMigrationShards();
        migrationShards.forEach(migrationShard -> {
            ShardMigrationResult result = randomResult();
            ((DefaultMigrationShard) migrationShard).updateShardMigrationResult(result);
            migrationShard.update(null, null);
        });


        MigrationEvent eventNew = migrationEventDao.buildMigrationEvent(2);
        MigrationCluster migrationClusterNew = eventNew.getMigrationClusters().get(0);
        Assert.assertEquals(migrationCluster.clusterName(), migrationClusterNew.clusterName());

        List<MigrationShard> migrationShardsNew = migrationClusterNew.getMigrationShards();
        Assert.assertEquals(migrationShards.size(), migrationShardsNew.size());

        for (int i = 0; i < migrationShards.size(); i++) {

            ShardMigrationResult result = migrationShards.get(i).getShardMigrationResult();
            ShardMigrationResult resultNew = migrationShardsNew.get(i).getShardMigrationResult();

            Assert.assertFalse(result == resultNew);
            Assert.assertEquals(result, resultNew);
        }
    }

    private ShardMigrationResult randomResult() {

        DefaultShardMigrationResult result = new DefaultShardMigrationResult();
        for (ShardMigrationStep step : ShardMigrationStep.values()) {
            int random = randomInt();
            boolean success = random % 2 == 0 ? true : false;
            result.updateStepResult(step, success, randomString(10));
        }
        return result;
    }


}
