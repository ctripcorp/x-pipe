package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

@RunWith(MockitoJUnitRunner.class)
public class MigrationPreMigratingStateTest extends AbstractMigrationStateTest {

    private MigrationPreMigratingState preMigratingState;

    @Before
    public void beforeMigrationPreMigratingStateTest() {
        preMigratingState = new MigrationPreMigratingState(migrationCluster);
        when(migrationCluster.getMigrationService().shouldMigrateSentinelBeacon(migrationCluster)).thenReturn(true);
    }

    @Test
    public void shouldContinueWhenPreMigrateSucceed() {
        when(migrationCluster.getMigrationService().preMigrateSentinelBeacon(migrationCluster))
                .thenReturn(RetMessage.createSuccessMessage("ok"));

        preMigratingState.getStateActionState().tryAction();

        verify(migrationCluster).updateStepResultForAllShards(eq(ShardMigrationStep.PRE_MIGRATING),
                eq(true), anyString());
        verify(migrationCluster).updateStat(isA(MigrationMigratingState.class));
        verify(migrationCluster).process();
    }

    @Test
    public void shouldContinueWhenPreMigrateFail() {
        when(migrationCluster.getMigrationService().preMigrateSentinelBeacon(migrationCluster))
                .thenReturn(RetMessage.createFailMessage("fail"));

        preMigratingState.getStateActionState().tryAction();

        verify(migrationCluster).updateStepResultForAllShards(eq(ShardMigrationStep.PRE_MIGRATING),
                eq(false), anyString());
        verify(migrationCluster).updateStat(isA(MigrationMigratingState.class));
        verify(migrationCluster).process();
    }

    @Test
    public void shouldSkipWhenSentinelBeaconNotInGray() {
        when(migrationCluster.getMigrationService().shouldMigrateSentinelBeacon(migrationCluster)).thenReturn(false);

        preMigratingState.getStateActionState().tryAction();

        verify(migrationCluster).updateStepResultForAllShards(eq(ShardMigrationStep.PRE_MIGRATING),
                eq(true), contains("no beacon"));
        verify(migrationCluster.getMigrationService(), never()).preMigrateSentinelBeacon(eq(migrationCluster));
        verify(migrationCluster).updateStat(isA(MigrationMigratingState.class));
        verify(migrationCluster).process();
    }
}
