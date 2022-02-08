package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.model.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 28, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractMigrationStateTest extends AbstractConsoleTest {

    @Mock
    protected MigrationCluster migrationCluster;


    @Before
    public void beforeAbstractMigrationStateTest() {

        when(migrationCluster.getScheduled()).thenReturn(scheduled);
        when(migrationCluster.getMigrationExecutor()).thenReturn(executors);
        when(migrationCluster.getOuterClientService()).thenReturn(OuterClientService.DEFAULT);

        List<MigrationShard> migrationShards = new LinkedList<>();
        int shardSize = getShardSize();

        for (int i = 0; i < shardSize; i++) {

            MigrationShard migrationShard = mock(MigrationShard.class);
            migrationShards.add(migrationShard);
        }

        when(migrationCluster.getMigrationShards()).thenReturn(migrationShards);
    }

    public int getShardSize() {
        return 4;
    }

    protected void shardResult(int failCount) {

        int currentFailCount = 0;

        for (MigrationShard migrationShard : migrationCluster.getMigrationShards()) {

            ShardMigrationResult result = mock(ShardMigrationResult.class);
            when(migrationShard.getShardMigrationResult()).thenReturn(result);

            when(result.stepTerminated(any())).thenReturn(true);
            if(currentFailCount < failCount){
                when(result.stepSuccess(any())).thenReturn(false);
                currentFailCount++;
            }else {
                when(result.stepSuccess(any())).thenReturn(true);
            }
        }
    }

    protected void mockCheckDone() {
        int shardSize = getShardSize();
        when(migrationCluster.stepStatus(ShardMigrationStep.CHECK)).thenReturn(new ClusterStepResult(shardSize, shardSize, shardSize));
    }




}
