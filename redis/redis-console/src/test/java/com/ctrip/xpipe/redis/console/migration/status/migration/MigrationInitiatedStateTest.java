package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 27, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationInitiatedStateTest extends AbstractMigrationStateTest {

    private MigrationInitiatedState initiatedState;

    @Override
    public void beforeAbstractMigrationStateTest() {
        when(migrationCluster.getScheduled()).thenReturn(scheduled);
        when(migrationCluster.getMigrationExecutor()).thenReturn(executors);
    }

    @Before
    public void beforeMigrationInitiatedStateTest() {
        initiatedState = new MigrationInitiatedState(migrationCluster);
    }

    @Test
    public void testRollback() {

        initiatedState.getStateActionState().tryRollback();
        verify(migrationCluster).updateStat(any(MigrationState.class));
        verify(migrationCluster).process();

    }
}
