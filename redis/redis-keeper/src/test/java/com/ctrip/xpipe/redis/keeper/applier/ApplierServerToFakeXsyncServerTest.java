package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 21:05
 */
public class ApplierServerToFakeXsyncServerTest extends AbstractRedisOpParserTest {

    private FakeXsyncServer server;

    private DefaultApplierServer applier;

    @Before
    public void setUp() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);

        applier = new DefaultApplierServer(
                "ApplierTest",
                new DefaultEndPoint("127.0.0.1", server.getPort()),
                new GtidSet(""),
                getXpipeNettyClientKeyedObjectPool(),
                parser,
                Executors.newScheduledThreadPool(
                        OsUtils.getCpuCount(), XpipeThreadFactory.create("reconnect-scheduler"))
        );
        applier.initialize();
        applier.start();
    }

    @Test
    public void test() {

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
