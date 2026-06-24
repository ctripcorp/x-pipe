package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.migration.exception.MigrationNoNeedException;
import com.ctrip.xpipe.redis.console.service.migration.support.HeteroMigrationSupport;
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

@RunWith(MockitoJUnitRunner.class)
public class MigrationPreCheckCmdHeteroTest extends AbstractConsoleTest {

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
    private ConsoleConfig config;
    @Mock
    private HeteroMigrationSupport heteroMigrationSupport;
    @Mock
    private MigrationSystemAvailableChecker.MigrationSystemAvailability availability;

    private MigrationPreCheckCmd preCheckCmd;
    private BeaconMigrationRequest migrationRequest;
    private ClusterTbl clusterTbl;
    private AzGroupClusterEntity shaOneWayAzGroup;
    private DcTbl jqDc;
    private DcTbl oyDc;

    @Before
    public void setUp() throws Exception {
        migrationRequest = new BeaconMigrationRequest();
        clusterTbl = new ClusterTbl().setId(14L).setClusterName("hetero-dual-oneway")
                .setClusterType(ClusterType.HETERO.name());
        shaOneWayAzGroup = new AzGroupClusterEntity().setId(23L).setActiveAzId(1L)
                .setAzGroupClusterType(ClusterType.ONE_WAY.name());
        jqDc = new DcTbl().setId(1L).setDcName("jq");
        oyDc = new DcTbl().setId(2L).setDcName("oy");

        preCheckCmd = new MigrationPreCheckCmd(migrationRequest, checker, configService, clusterService, dcCache,
                beaconMetaService, config, heteroMigrationSupport);

        migrationRequest.setClusterName("hetero-dual-oneway");
        migrationRequest.setFailoverGroups(Sets.newHashSet("hetero-dual-oneway_jq_1+jq"));
        migrationRequest.setGroups(new HashSet<MonitorGroupMeta>() {{
            add(new MonitorGroupMeta("hetero-dual-oneway_jq_1+jq", "jq", Collections.emptySet(), false));
            add(new MonitorGroupMeta("hetero-dual-oneway_jq_1+oy", "oy", Collections.emptySet(), true));
        }});

        when(checker.getResult()).thenReturn(availability);
        when(availability.isAvaiable()).thenReturn(true);
        when(configService.allowAutoMigration()).thenReturn(true);
        when(clusterService.find("hetero-dual-oneway")).thenReturn(clusterTbl);
        when(heteroMigrationSupport.isHeteroCluster(clusterTbl)).thenReturn(true);
        when(heteroMigrationSupport.resolveAzGroupClusterForBeaconRequest(clusterTbl, migrationRequest))
                .thenReturn(shaOneWayAzGroup);
        when(dcCache.find(1L)).thenReturn(jqDc);
        when(beaconMetaService.compareDrBeaconMetaWithXPipe(eq("hetero-dual-oneway"), anySet()))
                .thenReturn(true);
    }

    @Test
    public void heteroPreCheckShouldUseAzGroupActiveDc() throws Throwable {
        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(shaOneWayAzGroup, migrationRequest.getAzGroupCluster());
        Assert.assertEquals(jqDc, migrationRequest.getSourceDcTbl());
    }

    @Test(expected = MigrationNoNeedException.class)
    public void heteroPreCheckShouldSkipWhenActiveDcNotFailed() throws Throwable {
        migrationRequest.setFailoverGroups(Sets.newHashSet("hetero-dual-oneway_jq_1+oy"));
        migrationRequest.setGroups(new HashSet<MonitorGroupMeta>() {{
            add(new MonitorGroupMeta("hetero-dual-oneway_jq_1+oy", "oy", Collections.emptySet(), false));
            add(new MonitorGroupMeta("hetero-dual-oneway_jq_1+jq", "jq", Collections.emptySet(), true));
        }});
        when(dcCache.find(1L)).thenReturn(jqDc);

        CommandFuture future = preCheckCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }
}
