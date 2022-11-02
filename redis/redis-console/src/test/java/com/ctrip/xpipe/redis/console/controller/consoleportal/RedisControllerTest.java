package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.KeeperContainerInfoModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RedisControllerTest extends AbstractConsoleIntegrationTest {
    @Autowired
    RedisController redisController;
    @Autowired
    KeeperContainerService keeperContainerService;
    @Autowired
    ClusterService clusterService;

    @Test
    public void testMigrationKeepersByAllClusters() {
        List<ClusterTbl> allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(7L);
        Assert.assertEquals(3, allClusterByKeeperContainer.size());

        MigrationKeeperModel model = new MigrationKeeperModel();
        model.setSrcKeeperContainer(keeperContainerService.findKeeperContainerInfoModelById(7));
        redisController.migrateKeepers(model);

        allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(7L);
        Assert.assertEquals(0, allClusterByKeeperContainer.size());
    }

    @Test
    public void testMigrationKeepersByDesignatedClusters() {
        List<ClusterTbl> allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(7L);
        Assert.assertEquals(3, allClusterByKeeperContainer.size());

        MigrationKeeperModel model = new MigrationKeeperModel();
        model.setSrcKeeperContainer(keeperContainerService.findKeeperContainerInfoModelById(7));
        List<ClusterTbl> clusters = new ArrayList<>();
        clusters.add(new ClusterTbl().setClusterName("hetero-cluster"));
        clusters.add(new ClusterTbl().setClusterName("hetero-local-cluster"));
        model.setMigrationClusters(clusters);
        redisController.migrateKeepers(model);

        allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(7L);
        Assert.assertEquals(1, allClusterByKeeperContainer.size());
    }

    @Test
    public void testMigrationKeepersByDesignatedClustersAndDesignatedMaxMigrateNum() {
        List<ClusterTbl> allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(7L);
        Assert.assertEquals(3, allClusterByKeeperContainer.size());

        MigrationKeeperModel model = new MigrationKeeperModel();
        model.setSrcKeeperContainer(keeperContainerService.findKeeperContainerInfoModelById(7));
        List<ClusterTbl> clusters = new ArrayList<>();
        clusters.add(new ClusterTbl().setClusterName("hetero-cluster"));
        clusters.add(new ClusterTbl().setClusterName("hetero-local-cluster"));
        model.setMigrationClusters(clusters);
        model.setMaxMigrationKeeperNum(2);
        redisController.migrateKeepers(model);

        allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(7L);
        Assert.assertEquals(2, allClusterByKeeperContainer.size());
    }

    @Test
    public void testMigrationKeepersFailByNullSrcKeeperContainer() {
        MigrationKeeperModel model = new MigrationKeeperModel();
        List<ClusterTbl> clusters = new ArrayList<>();
        clusters.add(new ClusterTbl().setClusterName("hetero-cluster"));
        clusters.add(new ClusterTbl().setClusterName("hetero-local-cluster"));
        model.setMigrationClusters(clusters);
        model.setMaxMigrationKeeperNum(2);
        try {
            redisController.migrateKeepers(model);
        } catch (Exception e) {
            Assert.assertEquals("src keeperContainer must not be null", e.getMessage());
        }

        KeeperContainerInfoModel srcKeeperContainer = new KeeperContainerInfoModel();
        srcKeeperContainer.setId(0L);
        model.setSrcKeeperContainer(srcKeeperContainer);
        try {
            redisController.migrateKeepers(model);
        } catch (Exception e) {
            Assert.assertEquals("src keeperContainer must not be null", e.getMessage());
        }
    }

    @Test
    public void testMigrationKeepersWithAz() {
        long keeperContainerId = 21L;
        List<ClusterTbl> allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(keeperContainerId);
        Assert.assertEquals(1, allClusterByKeeperContainer.size());

        MigrationKeeperModel model = new MigrationKeeperModel();
        model.setSrcKeeperContainer(keeperContainerService.findKeeperContainerInfoModelById(keeperContainerId));

        redisController.migrateKeepers(model);
        allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(keeperContainerId);
        Assert.assertEquals(0, allClusterByKeeperContainer.size());
    }

    @Test
    public void testMigrationKeepersByDesignatedClustersMaxMigrateNumAndTargetKeeperContainer() {
        List<ClusterTbl> allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(7L);
        Assert.assertEquals(3, allClusterByKeeperContainer.size());
        allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(10L);
        Assert.assertEquals(0, allClusterByKeeperContainer.size());

        MigrationKeeperModel model = new MigrationKeeperModel();
        model.setSrcKeeperContainer(keeperContainerService.findKeeperContainerInfoModelById(7));
        List<ClusterTbl> clusters = new ArrayList<>();
        clusters.add(new ClusterTbl().setClusterName("hetero-cluster"));
        clusters.add(new ClusterTbl().setClusterName("hetero-local-cluster"));
        model.setMigrationClusters(clusters);
        model.setTargetKeeperContainer(keeperContainerService.findKeeperContainerInfoModelById(10L));
        redisController.migrateKeepers(model);

        allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(7L);
        Assert.assertEquals(1, allClusterByKeeperContainer.size());
        allClusterByKeeperContainer = clusterService.findAllClusterByKeeperContainer(10L);
        Assert.assertEquals(2, allClusterByKeeperContainer.size());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migrate-keeper-test.sql");
    }
}