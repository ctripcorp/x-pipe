package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationProgress;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/5/5
 */
public class MigrationProgressTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationService migrationService;

    @Test
    public void testBuildProgress() {
        MigrationProgress progress = migrationService.buildMigrationProgress(1);
        Assert.assertEquals(3, progress.getTotal());
        Assert.assertEquals(1, progress.getSuccess());
        Assert.assertEquals(10, progress.getAvgMigrationSeconds());
        Assert.assertEquals(1, progress.getProcess());
        Assert.assertEquals(1, progress.getInit());
        Assert.assertEquals(0, progress.getFail());
        Assert.assertEquals(new HashMap<String, Long>() {{
            put("jq", 2L);
            put("oy", 1L);
            put("fra", 0L);
        }}, progress.getActiveDcs());
        Assert.assertEquals(new HashMap<MigrationStatus, Long>() {{
            put(MigrationStatus.Success, 1L);
            put(MigrationStatus.Initiated, 1L);
            put(MigrationStatus.PartialSuccess, 1L);
        }}, progress.getMigrationStatuses());
    }

    @Test
    public void testGetLatestMigrationOperators() {
        Set<String> latestMigrationOperators = migrationService.getLatestMigrationOperators(1);
        Assert.assertEquals(3, latestMigrationOperators.size());
        Assert.assertEquals(Sets.newHashSet("xpipe", "beacon", "some"), latestMigrationOperators);
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-progress.sql");
    }
}
