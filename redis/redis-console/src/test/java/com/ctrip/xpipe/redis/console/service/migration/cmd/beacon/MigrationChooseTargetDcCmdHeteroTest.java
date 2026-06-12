package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.MigrationCrossZoneException;
import com.ctrip.xpipe.redis.console.service.migration.support.HeteroMigrationSupport;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MigrationChooseTargetDcCmdHeteroTest extends AbstractConsoleTest {

    @Mock
    private DcCache dcCache;
    @Mock
    private DcClusterService dcClusterService;
    @Mock
    private DcRelationsService dcRelationsService;
    @Mock
    private HeteroMigrationSupport heteroMigrationSupport;

    private BeaconMigrationRequest migrationRequest;
    private MigrationChooseTargetDcCmd chooseTargetDcCmd;
    private ClusterTbl clusterTbl;
    private AzGroupClusterEntity shaOneWayAzGroup;
    private DcTbl jqDc;
    private DcTbl fraDc;

    @Before
    public void setUp() {
        migrationRequest = new BeaconMigrationRequest();
        chooseTargetDcCmd = new MigrationChooseTargetDcCmd(migrationRequest, dcCache, dcClusterService,
                dcRelationsService, heteroMigrationSupport);

        clusterTbl = new ClusterTbl().setId(14L).setClusterName("hetero-dual-oneway")
                .setClusterType(ClusterType.HETERO.name());
        shaOneWayAzGroup = new AzGroupClusterEntity().setId(23L);
        jqDc = new DcTbl().setId(1L).setDcName("jq").setZoneId(1);
        fraDc = new DcTbl().setId(3L).setDcName("fra").setZoneId(2);

        migrationRequest.setClusterTbl(clusterTbl);
        migrationRequest.setSourceDcTbl(jqDc);
        migrationRequest.setAzGroupCluster(shaOneWayAzGroup);
        migrationRequest.setIsForced(true);
        migrationRequest.setTargetIDC("fra");

        when(dcCache.find("fra")).thenReturn(fraDc);
        when(dcClusterService.find(3L, 14L)).thenReturn(new DcClusterTbl());
        when(heteroMigrationSupport.isHeteroCluster(clusterTbl)).thenReturn(true);
        when(heteroMigrationSupport.isSameAzGroup(14L, "jq", "fra")).thenReturn(false);
    }

    @Test(expected = MigrationCrossZoneException.class)
    public void heteroForcedTargetShouldRejectCrossAzGroupDc() throws Throwable {
        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Test
    public void heteroAvailableDcsShouldFilterByAzGroup() throws Throwable {
        migrationRequest.setIsForced(false);
        migrationRequest.setAvailableDcs(Sets.newHashSet("oy", "fra"));
        when(heteroMigrationSupport.filterDcsInSameAzGroup(shaOneWayAzGroup, migrationRequest.getAvailableDcs()))
                .thenReturn(Sets.newHashSet("oy"));
        when(dcRelationsService.getClusterTargetDcByPriority(14L, "hetero-dual-oneway", "jq",
                java.util.Collections.singletonList("oy"))).thenReturn("oy");
        DcTbl oyDc = new DcTbl().setId(2L).setDcName("oy");
        when(dcCache.find("oy")).thenReturn(oyDc);

        CommandFuture future = chooseTargetDcCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(oyDc, migrationRequest.getTargetDcTbl());
    }
}
