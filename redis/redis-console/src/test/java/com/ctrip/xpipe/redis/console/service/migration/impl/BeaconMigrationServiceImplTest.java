package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class BeaconMigrationServiceImplTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private BeaconMigrationServiceImpl migrationService;

    @Autowired
    private BeaconMetaServiceImpl beaconMetaService;

    private MetaCache metaCache;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/beacon-migration-test.sql");
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "beacon-migration-test.xml";
    }

    @Before
    public void setupBeaconMigrationServiceImplTest() throws Exception {
        metaCache = Mockito.mock(MetaCache.class);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.doAnswer(invocation -> {
            String activeDc = invocation.getArgument(0, String.class);
            String backupDc = invocation.getArgument(1, String.class);
            XpipeMeta xpipeMeta = getXpipeMeta();
            return !xpipeMeta.getDcs().get(activeDc).getZone().equals(xpipeMeta.getDcs().get(backupDc).getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        beaconMetaService.setMetaCache(metaCache);
    }

    @DirtiesContext
    @Test(expected = ClusterMigrationNotSuccessException.class)
    public void testMigrateNormalCluster() throws Throwable {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet("jq"));
        CommandFuture future = migrationService.migrate(request);
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause().getCause();
    }

    @Test(expected = ClusterMigrationNotSuccessException.class)
    public void testMigrateCheckingCluster() throws Throwable {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster2", Sets.newHashSet("jq"));
        CommandFuture future = migrationService.migrate(request);
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause().getCause();
    }

    @Test(expected = ClusterMigrationNotSuccessException.class)
    public void testMigrateMigratingCluster() throws Throwable {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster5", Sets.newHashSet("jq"));
        CommandFuture future = migrationService.migrate(request);
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause().getCause();
    }

    @DirtiesContext
    @Test(expected = ClusterMigrationNotSuccessException.class)
    public void testForcedMigrate() throws Throwable {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet());
        request.setIsForced(true);
        request.setTargetIDC("oy");

        CommandFuture future = migrationService.migrate(request);
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause().getCause();
    }

    private BeaconMigrationRequest buildBeaconMigrationRequest(String clusterName, Set<String> failDcs) {
        BeaconMigrationRequest request = new BeaconMigrationRequest();
        Set<MonitorGroupMeta> groups = beaconMetaService.buildBeaconGroups(clusterName);
        Set<String> failoverGroups = new HashSet<>();

        request.setClusterName(clusterName);

        groups.forEach(group -> {
            if (failDcs.contains(group.getIdc())) {
                group.setDown(true);
                failoverGroups.add(group.getName());
            } else {
                group.setDown(false);
            }
        });

        request.setFailoverGroups(failoverGroups);
        request.setGroups(groups);

        return request;
    }

}
