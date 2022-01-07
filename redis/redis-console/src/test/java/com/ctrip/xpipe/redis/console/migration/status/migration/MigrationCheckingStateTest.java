package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.migration.AbstractOuterClientService;
import com.ctrip.xpipe.redis.console.migration.model.ClusterStepResult;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

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
    public void testCheckCRedisFail(){

        when(migrationCluster.getOuterClientService()).thenReturn(new AbstractOuterClientService() {
            @Override
            public boolean clusterMigratePreCheck(String clusterName) throws OuterClientException {
                return true;
            }

            @Override
            public DcMeta getOutClientDcMeta(String dc) throws Exception {
                return null;
            }
        });

        checkingState.action();
        verify(migrationCluster, never()).markCheckFail(anyString());

        when(migrationCluster.getOuterClientService()).thenReturn(new AbstractOuterClientService() {
            @Override
            public boolean clusterMigratePreCheck(String clusterName) throws OuterClientException {
                return false;
            }

            @Override
            public DcMeta getOutClientDcMeta(String dc) throws Exception {
                return null;
            }
        });

        checkingState.action();
        verify(migrationCluster).markCheckFail(anyString());
    }


    @Test
    public void testCheckSuccess() {

        int shardSize = getShardSize();
        when(migrationCluster.stepStatus(ShardMigrationStep.CHECK)).thenReturn(new ClusterStepResult(shardSize, shardSize, shardSize));

        checkingState.getStateActionState().tryAction();
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

        checkingState.getStateActionState().tryAction();
        checkingState.refresh();

        sleep(50);
        migrationCluster.getMigrationShards().forEach(migrationShard -> verify(migrationShard).doCheck());
        verify(migrationCluster, times(0)).updateStat(isA(MigrationMigratingState.class));
        verify(migrationCluster, times(0)).process();

        checkingState.getStateActionState().tryRollback();

        verify(migrationCluster).updateStat(isA(MigrationAbortedState.class));
        verify(migrationCluster).process();

    }
}
