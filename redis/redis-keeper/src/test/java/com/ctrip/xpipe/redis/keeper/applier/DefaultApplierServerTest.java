package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.lwm.DefaultLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultXsyncReplication;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
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
                applierMeta, leaderElectorManager, parser);
        server.initialize();

        assertTrue(server.sequence.getLifecycleState().isInitialized());
        assertTrue(server.lwmManager.getLifecycleState().isInitialized());
        assertTrue(server.replication.getLifecycleState().isInitialized());

        assertNotNull(server.client);
        assertNotNull(server.parser);

        assertEquals(server.client, ((DefaultLwmManager) server.lwmManager).client);
        assertEquals(server.lwmManager, ((DefaultSequenceController) server.sequence).lwmManager);
        assertEquals(server.parser, ((DefaultCommandDispatcher) server.dispatcher).parser);

        assertEquals(server.gtid_executed, ((DefaultXsyncReplication) server.replication).gtid_executed);
        assertEquals(server.gtid_executed, ((DefaultCommandDispatcher) server.dispatcher).gtid_executed);
        assertEquals(server.gtid_executed, ((DefaultLwmManager) server.lwmManager).gtid_executed);

        //server.client.close()
    }
}