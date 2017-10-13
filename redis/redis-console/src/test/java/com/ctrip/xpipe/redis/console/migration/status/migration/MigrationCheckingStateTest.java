package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.ClusterStepResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 28, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationCheckingStateTest extends AbstractMigrationStateTest {


    private MigrationCheckingState checkingState;

    @Before
    public void beforeMigrationCheckingStateTest() {

        checkingState = new MigrationCheckingState(migrationCluster);

    }


    @Test
    public void testCheckSuccess() {

        int shardSize = getShardSize();
        when(migrationCluster.stepStatus(ShardMigrationStep.CHECK)).thenReturn(new ClusterStepResult(shardSize, shardSize, shardSize));

        checkingState.action();
        checkingState.refresh();

        sleep(50);
        migrationCluster.getMigrationShards().forEach(migrationShard -> verify(migrationShard).doCheck());

        verify(migrationCluster).updateStat(isA(MigrationMigratingState.class));
        verify(migrationCluster).process();
    }

    @Test
    public void testCheckRollback() {


        int shardSize = getShardSize();

        when(migrationCluster.stepStatus(ShardMigrationStep.CHECK)).thenReturn(new ClusterStepResult(shardSize, shardSize, shardSize/2));

        checkingState.action();
        checkingState.refresh();

        sleep(50);
        migrationCluster.getMigrationShards().forEach(migrationShard -> verify(migrationShard).doCheck());
        verify(migrationCluster, times(0)).updateStat(isA(MigrationMigratingState.class));
        verify(migrationCluster, times(0)).process();

        checkingState.rollback();

        verify(migrationCluster).updateStat(isA(MigrationAbortedState.class));
        verify(migrationCluster).process();

    }
}
