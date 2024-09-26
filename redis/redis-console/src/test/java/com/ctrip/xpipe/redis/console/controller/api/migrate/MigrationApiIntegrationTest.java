package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.redis.checker.impl.TestMetaCache;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.ClusterMigrationStatus;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.ClusterMigrationStatusV2;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationHistoryReq;
import com.ctrip.xpipe.redis.console.resources.DefaultMetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2024/3/18
 */
public class MigrationApiIntegrationTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationApi migrationApi;

    @Autowired
    private MetaCache metaCache;

    @Before
    public void setupMigrationApiIntegrationTest() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta("jq");
        ClusterMeta clusterMeta = new ClusterMeta("bi_cluster1");
        clusterMeta.setDcs("jq,oy");
        dcMeta.addCluster(clusterMeta);
        xpipeMeta.addDc(dcMeta);

        ((TestMetaCache) metaCache).setXpipeMeta(xpipeMeta);
    }

    @Test
    public void testGetClusterMigrationHistory() {
        MigrationHistoryReq req = new MigrationHistoryReq();
        req.from = 0;
        req.to = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + 600;
        req.clusters = Collections.singleton("cluster2");

        Map<String, List<ClusterMigrationStatus>> resp = migrationApi.getClusterMigrationHistory(req);
        logger.info("[testGetClusterMigrationHistory] {}", resp);
        Assert.assertTrue(resp.containsKey("cluster2"));
        Assert.assertEquals(1, resp.get("cluster2").size());
        Assert.assertEquals("Processing", resp.get("cluster2").get(0).status);
    }

    @Test
    public void testGetClusterMigrationHistoryV2() {
        MigrationHistoryReq req = new MigrationHistoryReq();
        req.from = 0;
        req.to = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + 600;
        req.clusters = new HashSet<>(Arrays.asList("cluster2", "bi_cluster1"));

        Map<String, List<ClusterMigrationStatusV2>> resp = migrationApi.getClusterMigrationHistoryV2(req);
        logger.info("[testGetClusterMigrationHistoryV2] {}", resp);
        Assert.assertTrue(resp.containsKey("cluster2"));
        Assert.assertEquals(1, resp.get("cluster2").size());
        Assert.assertEquals("Processing", resp.get("cluster2").get(0).status);
        Assert.assertEquals(Collections.singleton("jq"), resp.get("cluster2").get(0).sourceDcs);
        Assert.assertEquals(Collections.singleton("oy"), resp.get("cluster2").get(0).destDcs);
        Assert.assertTrue(resp.containsKey("bi_cluster1"));
        Assert.assertEquals(1, resp.get("bi_cluster1").size());
        Assert.assertEquals("Success", resp.get("bi_cluster1").get(0).status);
        Assert.assertEquals(new HashSet<>(Arrays.asList("jq", "oy")), resp.get("bi_cluster1").get(0).sourceDcs);
        Assert.assertEquals(Collections.singleton("oy"), resp.get("bi_cluster1").get(0).destDcs);
    }

    @Test
    public void testOnlyGetOneWayClusterMigrationHistoryV2() {
        MigrationHistoryReq req = new MigrationHistoryReq();
        req.from = 0;
        req.to = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + 600;
        req.clusters = new HashSet<>(Arrays.asList("cluster2"));

        Map<String, List<ClusterMigrationStatusV2>> resp = migrationApi.getClusterMigrationHistoryV2(req);
        Assert.assertEquals(1, resp.size());
        logger.info("[testGetClusterMigrationHistoryV2] {}", resp);
        Assert.assertTrue(resp.containsKey("cluster2"));
        Assert.assertEquals(1, resp.get("cluster2").size());
        Assert.assertEquals("Processing", resp.get("cluster2").get(0).status);
        Assert.assertEquals(Collections.singleton("jq"), resp.get("cluster2").get(0).sourceDcs);
        Assert.assertEquals(Collections.singleton("oy"), resp.get("cluster2").get(0).destDcs);
    }

    @Test
    public void testOnlyGetBiClusterMigrationHistoryV2() {
        MigrationHistoryReq req = new MigrationHistoryReq();
        req.from = 0;
        req.to = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + 600;
        req.clusters = new HashSet<>(Arrays.asList("bi_cluster1"));

        Map<String, List<ClusterMigrationStatusV2>> resp = migrationApi.getClusterMigrationHistoryV2(req);
        Assert.assertEquals(1, resp.size());
        Assert.assertTrue(resp.containsKey("bi_cluster1"));
        Assert.assertEquals(1, resp.get("bi_cluster1").size());
        Assert.assertEquals("Success", resp.get("bi_cluster1").get(0).status);
        Assert.assertEquals(new HashSet<>(Arrays.asList("jq", "oy")), resp.get("bi_cluster1").get(0).sourceDcs);
        Assert.assertEquals(Collections.singleton("oy"), resp.get("bi_cluster1").get(0).destDcs);
    }

    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-test.sql");
    }

}
