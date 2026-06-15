package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.migration.command.ReactorMigrationCommandBuilderImpl;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterMigrationNotSuccessException;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class BeaconMigrationServiceImplTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private BeaconMigrationServiceImpl migrationService;

    @Autowired
    private BeaconMetaServiceImpl beaconMetaService;

    @MockBean
    private ReactorMigrationCommandBuilderImpl reactorMigrationCommandBuilder;

    private MetaCache metaCache;

    private DcRelationsService dcRelationsService;

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

        Mockito.when(metaCache.getActiveDc(Mockito.anyString())).thenReturn("jq");
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
        beaconMetaService.setMetaCache(metaCache);

        dcRelationsService = Mockito.mock(DcRelationsService.class);
        Mockito.when(dcRelationsService.getClusterTargetDcByPriority(Mockito.anyLong(), Mockito.anyString(), Mockito.anyString(), Mockito.anyList())).thenReturn("oy");
        migrationService.setDcRelationsService(dcRelationsService);

        Mockito.when(reactorMigrationCommandBuilder.buildDcCheckCommand(anyString(), anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> failCheckCommand());
        Mockito.when(reactorMigrationCommandBuilder.buildPrevPrimaryDcCommand(anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> successPrevPrimaryDcCommand());
        Mockito.when(reactorMigrationCommandBuilder.buildNewPrimaryDcCommand(anyString(), anyString(), anyString(),
                        Mockito.<Supplier<MetaServerConsoleService.PreviousPrimaryDcMessage>>any()))
                .thenAnswer(invocation -> failNewPrimaryDcCommand());
    }

    private AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage> failCheckCommand() {
        return new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {
            @Override
            public String getName() {
                return "Mocked-Beacon-CheckFail";
            }

            @Override
            protected void doExecute() throws Exception {
                future().setSuccess(new MetaServerConsoleService.PrimaryDcCheckMessage(
                        MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT.FAIL, "mocked check fail"));
            }

            @Override
            protected void doReset() {
            }
        };
    }

    private AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage> successPrevPrimaryDcCommand() {
        return new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>() {
            @Override
            public String getName() {
                return "Mocked-Beacon-PrevPrimaryDcSuccess";
            }

            @Override
            protected void doExecute() throws Exception {
                future().setSuccess(new MetaServerConsoleService.PreviousPrimaryDcMessage(null, null, "mocked prev success"));
            }

            @Override
            protected void doReset() {
            }
        };
    }

    private AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage> failNewPrimaryDcCommand() {
        return new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
            @Override
            public String getName() {
                return "Mocked-Beacon-NewPrimaryDcFail";
            }

            @Override
            protected void doExecute() throws Exception {
                future().setSuccess(new MetaServerConsoleService.PrimaryDcChangeMessage(
                        MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.FAIL, "mocked new primary dc fail"));
            }

            @Override
            protected void doReset() {
            }
        };
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
        String activeDc = metaCache.getActiveDc(clusterName);
        Set<MonitorGroupMeta> groups = beaconMetaService.buildDrBeaconGroups(clusterName, activeDc);
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

    private void assertArrayEqualsDespiteOrder(String[] expected, String[] result) {
        assertEquals(expected.length, result.length);
        for (String e : expected) {
            boolean contains = false;
            for (String r : result) {
                if (e.equals(r)) {
                    contains = true;
                }
            }
            assertTrue(contains);
        }
    }

    @Test
    public void testExcludeChoiceInPriority() {
        Set<MonitorGroupMeta> groups = new HashSet<>();
        groups.add(buildSimpleGroup("A", "SHa-ALI", false));
        groups.add(buildSimpleGroup("B", "SHArB", false));
        groups.add(buildSimpleGroup("C", "SHaXY", true));
        groups.add(buildSimpleGroup("d", "SHAjQ", false));

        Mockito.when(dcRelationsService.getExcludedDcsForBiCluster(Mockito.anyString(), Mockito.anySet(), Mockito.anySet())).thenReturn(new HashSet<>());

        try {
            migrationService.decideExcludes("test", groups);
        } catch (Exception e) {
            assertTrue(e instanceof XpipeRuntimeException);
            assertTrue(e.getMessage().contains("cannot make a choice"));
        }

        Mockito.when(dcRelationsService.getExcludedDcsForBiCluster(Mockito.anyString(), Mockito.anySet(), Mockito.anySet())).thenReturn(Sets.newHashSet("SHAXY", "SHAJQ"));
        assertArrayEqualsDespiteOrder(new String[]{"SHAXY", "SHAJQ"}, migrationService.decideExcludes("test", groups));
    }

    private MonitorGroupMeta buildSimpleGroup(String name, String idc, boolean isDown) {
        MonitorGroupMeta group = new MonitorGroupMeta(name, idc, null, true);
        group.setDown(isDown);
        return group;
    }
}
