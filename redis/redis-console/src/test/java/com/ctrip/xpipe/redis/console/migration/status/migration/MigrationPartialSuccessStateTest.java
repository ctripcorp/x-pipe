package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 27, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationPartialSuccessStateTest extends AbstractMigrationStateTest{

    private MigrationPartialSuccessState partialSuccessState;

    @Before
    public void beforeMigrationPartialSuccessStateTest(){

        partialSuccessState = new MigrationPartialSuccessState(migrationCluster);

    }

    @Test
    public void testAlreadySuccessAction(){

        shardResult(0);

        partialSuccessState.getStateActionState().tryAction();

        verify(migrationCluster).updateStat(isA(MigrationPublishState.class));
        verify(migrationCluster).process();
        Assert.assertNotNull(migrationCluster.getOuterClientService());
    }

    @Test
    public void testSingleThreadProcess() throws Exception {
        // single worker thread + unbounded queue: tasks run serially on one pool thread.
        // CallerRunsPolicy with a tiny queue is NOT single-threaded — submitter may run tasks concurrently.
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        when(migrationCluster.getMigrationExecutor()).thenReturn(singleThreadExecutor);
        partialSuccessState = new MigrationPartialSuccessState(migrationCluster);

        int failCnt = 4;
        shardResult(failCnt);
        // failed shards are retrying: step not terminated yet, so refresh() won't treat them as final failure
        for (MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
            ShardMigrationResult result = migrationShard.getShardMigrationResult();
            when(result.stepTerminated(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)).thenReturn(false);
        }
        AtomicInteger retryCnt = new AtomicInteger(0);

        for (MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
            ShardMigrationResult result = migrationShard.getShardMigrationResult();
            doAnswer(invocation -> {
                ShardMigrationStep step = invocation.getArgument(0, ShardMigrationStep.class);
                when(result.stepTerminated(step)).thenReturn(false);
                return null;
            }).when(result).stepRetry(any(ShardMigrationStep.class));

            doAnswer(invocation -> {
                ShardMigrationResult newResult = mock(ShardMigrationResult.class);
                when(migrationShard.getShardMigrationResult()).thenReturn(newResult);
                when(newResult.stepTerminated(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)).thenReturn(true);
                when(newResult.stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)).thenReturn(true);
                partialSuccessState.refresh();
                retryCnt.incrementAndGet();
                return null;
            }).when(migrationShard).doMigrate();

            doAnswer(invocation -> {
                ShardMigrationResult migrateResult = migrationShard.getShardMigrationResult();
                when(migrateResult.stepTerminated(ShardMigrationStep.MIGRATE)).thenReturn(true);
                partialSuccessState.refresh();
                return null;
            }).when(migrationShard).doMigrateOtherDc();
        }

        partialSuccessState.getStateActionState().tryAction();

        waitConditionUntilTimeOut(() -> retryCnt.get() >= failCnt);
        verify(migrationCluster, Mockito.timeout(3000).times(1)).updateStat(isA(MigrationPublishState.class));
        verify(migrationCluster, Mockito.timeout(3000).times(1)).process();
        singleThreadExecutor.shutdown();
    }

    @Test
    public void testRollback(){

        shardResult(1);

        partialSuccessState.getStateActionState().tryAction();
        partialSuccessState.refresh();

        verify(migrationCluster).updateStat(isA(MigrationPartialRetryFailState.class));
        verify(migrationCluster, times(0)).process();

        partialSuccessState.getStateActionState().tryRollback();
        verify(migrationCluster).updateStat(isA(MigrationPartialSuccessRollBackState.class));
        verify(migrationCluster).process();

    }

    @Test
    public void testForcePublish(){

        shardResult(1);

        partialSuccessState.getStateActionState().tryAction();

        partialSuccessState.refresh();

        verify(migrationCluster).updateStat(isA(MigrationPartialRetryFailState.class));
        verify(migrationCluster, times(0)).process();

        logger.info("[testForcePublish]");

        partialSuccessState.updateAndForceProcess();
        verify(migrationCluster).updateStat(isA(MigrationForcePublishState.class));
        verify(migrationCluster).process();
    }

}
