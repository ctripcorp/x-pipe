package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/4/19
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationPreCheckCmdTest extends AbstractConsoleTest {

    private MigrationPreCheckCmd preCheckCmd;

    @Mock
    private MigrationSystemAvailableChecker checker;

    @Mock
    private ConfigService configService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private DcCache dcCache;

    @Mock
    private BeaconMetaService beaconMetaService;

    @Mock
    private MigrationSystemAvailableChecker.MigrationSystemAvailability availability;

    @Mock
    private ConsoleConfig config;

    private BeaconMigrationRequest migrationRequest;

    private String clusterName = "cluster1";

    private String dcName = "dc1";

    private String targetDcName = "dc2";

    private ClusterTbl clusterTbl;

    private DcTbl dcTbl;

    @Before
    public void setupMigrationPreCheckCmdTest() throws Exception {
        migrationRequest = new BeaconMigrationRequest();
        clusterTbl = new ClusterTbl();
        dcTbl = new DcTbl();
        preCheckCmd = new MigrationPreCheckCmd(migrationRequest, checker, configService, clusterService, dcCache, beaconMetaService, config);

        dcTbl.setDcName(dcName);
        migrationRequest.setFailoverGroups(Sets.newHashSet("shard1+dc1"));
        migrationRequest.setClusterName(clusterName);
        migrationRequest.setGroups(new HashSet<MonitorGroupMeta>() {{
            add(new MonitorGroupMeta("shard1+dc1", "dc1", Collections.emptySet(), false));
            add(new MonitorGroupMeta("shard1+dc2", "dc2", Collections.emptySet(), true));
        }});
        clusterTbl.setClusterType(ClusterType.ONE_WAY.name());
        when(checker.getResult()).thenReturn(availability);
        when(availability.isAvaiable()).thenReturn(true);
        when(clusterService.find(clusterName)).thenReturn(clusterTbl);
        when(dcCache.find(anyLong())).thenReturn(dcTbl);
        when(beaconMetaService.compareMetaWithXPipe(eq(clusterName), anySet())).thenReturn(true);
        when(configService.allowAutoMigration()).thenReturn(true);
    }

    @Test
    public void testPreCheckSuccess() throws Throwable {
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(clusterTbl, migrationRequest.getClusterTbl());
        Assert.assertEquals(dcTbl, migrationRequest.getSourceDcTbl());
    }

    @Test
    public void testMigrationSystemHealthIgnore() throws Throwable {
        when(availability.isAvaiable()).thenReturn(false);
        when(configService.ignoreMigrationSystemAvailability()).thenReturn(true);
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(clusterTbl, migrationRequest.getClusterTbl());
        Assert.assertEquals(dcTbl, migrationRequest.getSourceDcTbl());
    }

    @Test(expected = MigrationSystemNotHealthyException.class)
    public void testDRSystemDown() throws Throwable {
        when(availability.isAvaiable()).thenReturn(false);
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = AutoMigrationNotAllowException.class)
    public void testAutoMigrationNotAllow() throws Throwable {
        when(configService.allowAutoMigration()).thenReturn(false);
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = ClusterNotFoundException.class)
    public void testClusterNotFound() throws Throwable {
        when(clusterService.find(clusterName)).thenReturn(null);
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = MigrationNotSupportException.class)
    public void testClusterTypeNotSupportMigration() throws Throwable {
        clusterTbl.setClusterType(ClusterType.BI_DIRECTION.name());
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = WrongClusterMetaException.class)
    public void testMigrationWrongMeta() throws Throwable {
        when(beaconMetaService.compareMetaWithXPipe(eq(clusterName), anySet())).thenReturn(false);
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = MigrationNoNeedException.class)
    public void testMigrationNoNeed() throws Throwable {
        migrationRequest.setFailoverGroups(Sets.newHashSet());
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

}
