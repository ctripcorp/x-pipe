package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.AbstractMigrationTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.mockito.Mockito.*;

import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 27, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationInitiatedStateTest extends AbstractMigrationTest {

    @Mock
    private MigrationCluster migrationCluster;

    private MigrationInitiatedState initiatedState;

    @Before
    public void beforeMigrationInitiatedStateTest() {

        initiatedState = new MigrationInitiatedState(migrationCluster);
    }

    @Test
    public void testRollback() {

        initiatedState.rollback();

        verify(migrationCluster).updateStat(any(MigrationState.class));
        verify(migrationCluster).process();

    }
}
