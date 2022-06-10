package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.Shard;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 21:05
 */
@RunWith(MockitoJUnitRunner.class)
public class ApplierServerToFakeXsyncServerTest extends AbstractRedisOpParserTest {

    private FakeXsyncServer server;

    private DefaultApplierServer applier;

    private ApplierMeta applierMeta;

    @Mock
    private LeaderElectorManager leaderElectorManager;

    @Mock
    private LeaderElector leaderElector;

    @Before
    public void setUp() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);
        applierMeta = new ApplierMeta();
        applierMeta.setPort(randomPort());
        leaderElectorManager = Mockito.mock(LeaderElectorManager.class);
        leaderElector = Mockito.mock(LeaderElector.class);
        when(leaderElectorManager.createLeaderElector(any())).thenReturn(leaderElector);

        applier = new DefaultApplierServer(
                "ApplierTest",
                ClusterId.from(1L), ShardId.from(1L),
                applierMeta, leaderElectorManager, parser);
        applier.initialize();
        applier.start();

        applier.setState(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("a1:1-10:15-20,b1:1-8"));
    }

    @Test
    public void test() throws TimeoutException {

        waitConditionUntilTimeOut(() -> 1 == server.slaveCount());

        server.propagate("gtid a1:21 set k1 v1");
        server.propagate("gtid a1:22 mset k1 v1 k2 v2");
        server.propagate("gtid a1:23 del k1 k2");

        server.propagate("gtid a1:24 set k3 v3");
        server.propagate("gtid a1:25 set k4 v4");
        server.propagate("gtid a1:26 set k1 v6");

        server.propagate("MULTI");
        server.propagate("set k13 v13");
        server.propagate("set k14 v14");
        server.propagate("set k15 v15");
        server.propagate("GTID a1:28");

        server.propagate("gtid a1:27 set k1 v7");
    }
}
