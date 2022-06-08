package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.keeper.applier.lwm.DefaultLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultCommandDispatcher;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.Test;

import java.util.concurrent.Executors;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 09:14
 */
public class DefaultApplierServerTest extends AbstractRedisOpParserTest {

    @Test
    public void testInit() throws Exception {

        DefaultApplierServer server = new DefaultApplierServer(
                "ApplierTest",
                new DefaultEndPoint("127.0.0.1", 6000),
                new GtidSet(""),
                getXpipeNettyClientKeyedObjectPool(),
                parser,
                Executors.newScheduledThreadPool(
                        OsUtils.getCpuCount(), XpipeThreadFactory.create("reconnect-scheduler"))
        );
        server.initialize();

        assertTrue(server.sequence.getLifecycleState().isInitialized());
        assertTrue(server.lwmManager.getLifecycleState().isInitialized());
        assertTrue(server.replication.getLifecycleState().isInitialized());

        assertNotNull(server.client);
        assertNotNull(server.parser);

        assertEquals(server.client, ((DefaultLwmManager) server.lwmManager).client);
        assertEquals(server.sequence, ((DefaultLwmManager) server.lwmManager).sequence);
        assertEquals(server.lwmManager, ((DefaultSequenceController) server.sequence).lwmManager);
        assertEquals(server.parser, ((DefaultCommandDispatcher) server.dispatcher).parser);

        //server.client.close()
    }
}