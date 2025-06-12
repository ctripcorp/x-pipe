package com.ctrip.xpipe.redis.console.migration.status.migration.statemachine;

import com.ctrip.xpipe.redis.console.migration.model.ClusterStepResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.status.migration.AbstractMigrationStateTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationAbortedState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCheckingState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;


/**
 * @author wenchao.meng
 *         <p>
 *         Aug 31, 2017
 */
public class StateMachineTest extends AbstractMigrationStateTest{

    private MigrationCheckingState migrationCheckingState;

    @Before
    public void beforeMigrationMigratingStateTest(){
        migrationCheckingState = new MigrationCheckingState(migrationCluster);
    }

    @Test
    public void testProcessOnce() throws Exception {

        migrationCheckingState.getStateActionState().tryAction();

        for (MigrationShard migrationShard: migrationCluster.getMigrationShards()) {
            waitConditionUntilTimeOut(() ->
                mockingDetails(migrationShard)
                        .getInvocations().stream()
                        .filter(invocation -> invocation.getMethod().getName().equals("doCheck"))
                        .count() == 1
            );
        }

        migrationCheckingState.getStateActionState().tryAction();
        migrationCluster.getMigrationShards().forEach(migrationShard -> {
            verify(migrationShard).doCheck();
        });
    }

    @Test
    public void testDone(){

        int shardSize = getShardSize();
        migrationCheckingState.getStateActionState().tryAction();
        when(migrationCluster.stepStatus(ShardMigrationStep.CHECK)).thenReturn(new ClusterStepResult(shardSize, shardSize -1 , shardSize -1 ));

        migrationCheckingState.refresh();
        Assert.assertTrue(migrationCheckingState.getStateActionState() instanceof Doing);

        mockCheckDone();
        migrationCheckingState.refresh();
        Assert.assertTrue(migrationCheckingState.getStateActionState() instanceof Done);

        migrationCheckingState.refresh();
    }

    @Test
    public void testTimeout() throws TimeoutException {

        int timeoutMilli = 100;
        migrationCheckingState.setMigrationWaitTimeMilli(timeoutMilli);

        migrationCheckingState.getStateActionState().tryAction();

        waitConditionUntilTimeOut(()->migrationCheckingState.getStateActionState() instanceof Done, timeoutMilli * 2);
        Assert.assertTrue(migrationCheckingState.getStateActionState() instanceof Done);
    }

    @Test
    public void testRollback(){

        migrationCheckingState.getStateActionState().tryRollback();

        Assert.assertTrue(migrationCheckingState.getStateActionState() instanceof Done);

        verify(migrationCluster).updateStat(isA(MigrationAbortedState.class));
    }

    @Test
    public void testRollbackThenDone(){

        migrationCheckingState.getStateActionState().tryRollback();
        mockCheckDone();
        migrationCheckingState.refresh();
    }

    @Test
    public void testDoneRedo(){

        migrationCheckingState.getStateActionState().tryAction();
        mockCheckDone();
        migrationCheckingState.refresh();

        migrationCheckingState.refresh();
    }
}
