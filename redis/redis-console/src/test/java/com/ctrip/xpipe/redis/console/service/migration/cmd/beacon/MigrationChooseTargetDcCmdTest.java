package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.MigrationConflictException;
import com.ctrip.xpipe.redis.console.service.migration.exception.MigrationCrossZoneException;
import com.ctrip.xpipe.redis.console.service.migration.exception.NoAvailableDcException;
import com.ctrip.xpipe.redis.console.service.migration.exception.UnknownTargetDcException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/4/19
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationChooseTargetDcCmdTest extends AbstractConsoleTest {

    private BeaconMigrationRequest migrationRequest;

    private MigrationChooseTargetDcCmd chooseTargetDcCmd;

    @Mock
    private DcCache dcCache;

    @Mock
    private DcClusterService dcClusterService;

    @Mock
    private DcRelationsService dcRelationsService;

    private MigrationClusterTbl migrationClusterTbl;

    private ClusterTbl clusterTbl;

    private DcTbl dc0;

    private DcTbl dc1;

    private DcTbl dc2;

    private DcTbl dc3;

    private DcTbl dc4;

    @Before
    public void setup() {
        migrationRequest = new BeaconMigrationRequest();
        chooseTargetDcCmd = new MigrationChooseTargetDcCmd(migrationRequest, dcCache, dcClusterService, dcRelationsService);
        migrationClusterTbl = new MigrationClusterTbl();
        clusterTbl = new ClusterTbl().setClusterName("cluster1").setId(1);
        dc0 = new DcTbl().setDcName("dc0").setId(1);
        dc1 = new DcTbl().setDcName("dc1").setId(2);
        dc2 = new DcTbl().setDcName("dc2").setId(3);
        dc3 = new DcTbl().setDcName("dc3").setId(4);
        dc4 = new DcTbl().setDcName("dc4").setId(5);

        migrationRequest.setClusterTbl(clusterTbl);
        migrationRequest.setClusterName("cluster1");
        migrationRequest.setSourceDcTbl(dc0);
        migrationClusterTbl.setDestinationDcId(dc2.getId());
        migrationRequest.setAvailableDcs(Sets.newHashSet(dc1.getDcName(), dc2.getDcName()));

        when(dcCache.find(dc1.getDcName())).thenReturn(dc1);
        when(dcCache.find(dc2.getId())).thenReturn(dc2);
        when(dcClusterService.find(2, 1)).thenReturn(new DcClusterTbl());
    }

    @Test
    public void testForcedDc() throws Throwable {
        migrationRequest.setIsForced(true);
        migrationRequest.setTargetIDC(dc1.getDcName());
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(dc1, migrationRequest.getTargetDcTbl());
    }

    @Test
    public void reuseCurrentDestDc() throws Throwable {
        migrationRequest.setCurrentMigrationCluster(migrationClusterTbl);
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(dc2, migrationRequest.getTargetDcTbl());
    }

    @Test
    public void chooseAvailableDc() throws Throwable {
        migrationRequest.setAvailableDcs(Collections.singleton(dc1.getDcName()));
        when(dcRelationsService.getClusterTargetDcByPriority(1,"cluster1",migrationRequest.getSourceDcTbl().getDcName(), Lists.newArrayList(migrationRequest.getAvailableDcs()))).thenReturn(dc1.getDcName());
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(dc1, migrationRequest.getTargetDcTbl());
    }

    @Test(expected = NoAvailableDcException.class)
    public void clusterRefuseMigration() throws Throwable {
        migrationRequest.setAvailableDcs(Collections.singleton(dc1.getDcName()));
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = UnknownTargetDcException.class)
    public void testUnknownForcedDc() throws Throwable {
        migrationRequest.setIsForced(true);
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = UnknownTargetDcException.class)
    public void testNoTargetedDcForCluster() throws Throwable {
        migrationRequest.setIsForced(true);
        migrationRequest.setTargetIDC(dc1.getDcName());
        when(dcClusterService.find(dc1.getId(), 1)).thenReturn(null);
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = MigrationCrossZoneException.class)
    public void testForcedDcCrossZone() throws Throwable {
        migrationRequest.setIsForced(true);
        migrationRequest.setTargetIDC(dc1.getDcName());
        dc0.setZoneId(1);
        dc1.setZoneId(2);
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = MigrationConflictException.class)
    public void testTargetDcConflict() throws Throwable {
        migrationRequest.setIsForced(true);
        migrationRequest.setTargetIDC(dc1.getDcName());
        migrationRequest.setCurrentMigrationCluster(migrationClusterTbl.setDestinationDcId(dc2.getId()));

        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = MigrationConflictException.class)
    public void testCurrentDestDcConflict() throws Throwable {
        migrationRequest.setAvailableDcs(Collections.emptySet());
        migrationRequest.setCurrentMigrationCluster(migrationClusterTbl);

        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = NoAvailableDcException.class)
    public void testNoAvailableDc() throws Throwable {
        migrationRequest.setAvailableDcs(Collections.emptySet());
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

}
