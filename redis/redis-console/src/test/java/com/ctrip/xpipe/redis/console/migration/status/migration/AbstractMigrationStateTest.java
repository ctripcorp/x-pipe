package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationResult;
import org.junit.Before;
import org.mockito.Mock;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 28, 2017
 */
public class AbstractMigrationStateTest extends AbstractConsoleTest {

    @Mock
    protected MigrationCluster migrationCluster;


    @Before
    public void beforeAbstractMigrationStateTest() {


        when(migrationCluster.getMigrationExecutor()).thenReturn(executors);

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


}
