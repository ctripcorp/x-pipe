package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author lishanglin
 * date 2021/4/23
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationStateUpdateStatTest extends AbstractConsoleTest {

    @Mock
    private MigrationCluster holder;

    @Mock
    private MigrationState nextState;

    private TestMigrationState migrationState;

    @Before
    public void setupMigrationStateUpdateStatTest() {
        Mockito.when(holder.getMigrationExecutor()).thenReturn(executors);
        Mockito.when(holder.getScheduled()).thenReturn(scheduled);
        Mockito.doThrow(new RuntimeException()).when(holder).updateStat(Mockito.any());
        migrationState = new TestMigrationState(holder, MigrationStatus.Initiated);
    }

    @Test
    public void testContinueAfterUpdateFail() {
        Mockito.when(nextState.getStatus()).thenReturn(MigrationStatus.Checking);
        migrationState.getStateActionState().tryAction();
        migrationState.updateAndProcess(nextState);
        Mockito.verify(holder, Mockito.times(1)).process();
    }

    @Test
    public void testStopAfterUpdateMigratingFail() throws Exception {
        Mockito.when(nextState.getStatus()).thenReturn(MigrationStatus.Migrating);
        migrationState.getStateActionState().tryAction();
        migrationState.action();
        migrationState.updateAndProcess(nextState);
        Mockito.verify(holder, Mockito.times(1)).stop();
    }

    private static class TestMigrationState extends AbstractMigrationState {

        public TestMigrationState(MigrationCluster holder, MigrationStatus status) {
            super(holder, status);
        }

        public void updateAndProcess(MigrationState state) {
            super.updateAndProcess(state);
        }

        @Override
        protected void doRollback() {

        }

        @Override
        protected void doAction() {
        }

        @Override
        public void refresh() {

        }
    }

}
