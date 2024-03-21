package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.ClusterMigrationStatus;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationHistoryReq;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2024/3/18
 */
public class MigrationApiIntegrationTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationApi migrationApi;

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

    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-test.sql");
    }

}
