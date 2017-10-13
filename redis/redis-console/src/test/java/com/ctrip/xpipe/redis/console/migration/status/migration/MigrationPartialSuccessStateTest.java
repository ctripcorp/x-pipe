package com.ctrip.xpipe.redis.console.migration.status.migration;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
    public void testSuccessAction(){

        shardResult(0);

        partialSuccessState.action();

        partialSuccessState.refresh();
        partialSuccessState.refresh();

        verify(migrationCluster).updateStat(isA(MigrationPublishState.class));
        verify(migrationCluster).process();
    }

    @Test
    public void testRollback(){

        shardResult(1);

        partialSuccessState.action();

        partialSuccessState.refresh();

        verify(migrationCluster).updateStat(isA(MigrationPartialSuccessState.class));
        verify(migrationCluster, times(0)).process();

        partialSuccessState.rollback();
        verify(migrationCluster).updateStat(isA(MigrationPartialSuccessRollBackState.class));
        verify(migrationCluster).process();

    }

    @Test
    public void testForcePublish(){

        shardResult(1);

        partialSuccessState.action();

        partialSuccessState.refresh();

        verify(migrationCluster).updateStat(isA(MigrationPartialSuccessState.class));
        verify(migrationCluster, times(0)).process();

        logger.info("[testForcePublish]");

        partialSuccessState.forcePublish();
        verify(migrationCluster).updateStat(isA(MigrationForcePublishState.class));
        verify(migrationCluster).process();
    }

}
