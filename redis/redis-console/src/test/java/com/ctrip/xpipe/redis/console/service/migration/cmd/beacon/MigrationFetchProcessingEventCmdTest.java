package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.MigrationJustFinishException;
import com.ctrip.xpipe.redis.console.service.migration.exception.UnexpectMigrationDataException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 *         date 2021/4/19
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationFetchProcessingEventCmdTest extends AbstractConsoleTest {

    private MigrationFetchProcessingEventCmd fetchProcessingEventCmd;

    private BeaconMigrationRequest migrationRequest;

    @Mock
    private MigrationClusterDao migrationClusterDao;

    @Mock
    private ClusterService clusterService;

    @Mock
    private DcCache dcCache;

    private ClusterTbl clusterTbl;

    private DcTbl sourceDcTbl;

    private MigrationClusterTbl migrationClusterTbl;

    @Before
    public void setup() {
        migrationRequest = new BeaconMigrationRequest();
        clusterTbl = new ClusterTbl();
        sourceDcTbl = new DcTbl();
        migrationClusterTbl = new MigrationClusterTbl();
        fetchProcessingEventCmd = new MigrationFetchProcessingEventCmd(migrationRequest, clusterService,
                migrationClusterDao, dcCache);

        migrationRequest.setClusterName("cluster1");
        migrationRequest.setClusterTbl(clusterTbl);
        migrationRequest.setSourceDcTbl(sourceDcTbl);
        clusterTbl.setId(1);
        clusterTbl.setClusterName("cluster1");
        clusterTbl.setStatus(ClusterStatus.Normal.name());
        sourceDcTbl.setDcName("dc1");

        when(dcCache.find(anyLong())).thenReturn(new DcTbl().setDcName("dc2"));
    }

    @Test
    public void testNoCurrentMigration() throws Exception {
        CommandFuture future = fetchProcessingEventCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertNull(migrationRequest.getCurrentMigrationCluster());
    }

    @Test
    public void testMigrationOnProcessing() throws Throwable {
        mockClusterOnMigration();
        migrationClusterTbl.setStatus(MigrationStatus.Migrating.name());
        CommandFuture future = fetchProcessingEventCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(migrationClusterTbl, migrationRequest.getCurrentMigrationCluster());
    }

    @Test(expected = MigrationJustFinishException.class)
    public void testMigrationJustFinish() throws Throwable {
        mockClusterOnMigration();
        when(clusterService.find("cluster1")).thenReturn(new ClusterTbl().setStatus(ClusterStatus.Normal.name()));
        migrationClusterTbl.setStatus(MigrationStatus.Success.name());
        CommandFuture future = fetchProcessingEventCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test(expected = UnexpectMigrationDataException.class)
    public void testMigrationDataConflict() throws Throwable {
        mockClusterOnMigration();
        when(clusterService.find("cluster1")).thenReturn(clusterTbl);
        migrationClusterTbl.setStatus(MigrationStatus.Success.name());
        CommandFuture future = fetchProcessingEventCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    private void mockClusterOnMigration() {
        clusterTbl.setStatus(ClusterStatus.Migrating.name());
        clusterTbl.setMigrationEventId(1);
        when(migrationClusterDao.findByEventIdAndClusterId(1, 1)).thenReturn(migrationClusterTbl);
    }

}
