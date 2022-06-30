package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.redis.console.migration.AbstractMigrationTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationModel;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class MigrationServiceImplPaginationTest extends AbstractMigrationTest {

    @Test
    public void testCountAll() {
        long count = migrationService.countAll();
        Assert.assertEquals(5, count);
    }

    @Test
    public void testCountAllByCluster() {
        long count = migrationService.countAllByCluster(2);
        Assert.assertEquals(3, count);
    }

    @Test
    public void testFind() {
        List<MigrationModel> models = migrationService.find(3, 0);
        Assert.assertEquals(3, models.size());
        Assert.assertEquals(5, models.get(0).getEvent().getId());
        Assert.assertEquals(MigrationStatus.TYPE_SUCCESS, models.get(0).getStatus());
        Assert.assertEquals(1, models.get(0).getClusters().size());
        Assert.assertEquals(4, models.get(1).getEvent().getId());
        Assert.assertEquals(MigrationStatus.TYPE_WARNING, models.get(1).getStatus());
        Assert.assertEquals(2, models.get(1).getClusters().size());
        Assert.assertEquals(3, models.get(2).getEvent().getId());
        Assert.assertEquals(MigrationStatus.TYPE_SUCCESS, models.get(2).getStatus());
        Assert.assertEquals(2, models.get(2).getClusters().size());
    }

    @Test
    public void testFindByCluster() {
        List<MigrationModel> models = migrationService.findByCluster(2, 1, 2);
        MigrationModel model = models.get(0);
        Assert.assertEquals(1, models.size());
        Assert.assertEquals(2, model.getEvent().getId());
        Assert.assertEquals("cluster2", model.getClusters().get(0));
    }

    @Test
    public void testCountWithoutTestClusters() {
        long count = migrationService.countAllWithoutTestCluster();
        Assert.assertEquals(4, count);
    }

    @Test
    public void testFindWithoutTestClusters() {
        List<MigrationModel> models = migrationService.findWithoutTestClusters(2, 0);
        Assert.assertEquals(2, models.size());
        Assert.assertEquals(4, models.get(0).getEvent().getId());
        Assert.assertEquals(MigrationStatus.TYPE_WARNING, models.get(0).getStatus());
        Assert.assertEquals(2, models.get(0).getClusters().size());
        Assert.assertEquals(3, models.get(1).getEvent().getId());
        Assert.assertEquals(MigrationStatus.TYPE_SUCCESS, models.get(1).getStatus());
        Assert.assertEquals(2, models.get(1).getClusters().size());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-service-impl-test.sql");
    }

}
