package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sync.DefaultCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 09:14
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultApplierServerTest extends AbstractRedisOpParserTest {

    private ApplierMeta applierMeta;

    @Mock
    private LeaderElectorManager leaderElectorManager;

    @Mock
    private LeaderElector leaderElector;

    @Test
    public void testInit() throws Exception {
        applierMeta = new ApplierMeta().setPort(randomPort());
        when(leaderElectorManager.createLeaderElector(any())).thenReturn(leaderElector);

        DefaultApplierServer server = new DefaultApplierServer(
                "ApplierTest", ClusterId.from(1L), ShardId.from(1L),
                applierMeta, leaderElectorManager, parser, new TestKeeperConfig());
        server.initialize();

        assertTrue(server.sequenceController.getLifecycleState().isInitialized());

        assertNotNull(server.client);
        assertNotNull(server.parser);

        assertEquals(server.parser, ((DefaultCommandDispatcher) server.dispatcher).parser);
        assertEquals(server.sequenceController, ((DefaultCommandDispatcher) server.dispatcher).sequenceController);

        assertEquals(server.gtid_executed, ((DefaultCommandDispatcher) server.dispatcher).gtid_executed);

        assertEquals(server.stateThread, ((DefaultSequenceController) server.sequenceController).stateThread);

        assertEquals(server.workerThreads, ((DefaultSequenceController) server.sequenceController).workerThreads);

        assertEquals(server.scheduled, ((DefaultSequenceController) server.sequenceController).scheduled);

        assertEquals(server.gtidDistanceThreshold, ((DefaultCommandDispatcher) server.dispatcher).gtidDistanceThreshold);

        assertEquals(server.offsetRecorder, ((DefaultCommandDispatcher) server.dispatcher).offsetRecorder);
        assertEquals(server.offsetRecorder, ((DefaultSequenceController) server.sequenceController).offsetRecorder);

        server.dispose();

        //server.client.close()
    }
}