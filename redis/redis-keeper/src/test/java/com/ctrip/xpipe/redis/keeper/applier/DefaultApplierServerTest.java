package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.keeper.applier.lwm.DefaultLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultXsyncReplication;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 09:14
 */
public class DefaultApplierServerTest extends AbstractRedisOpParserTest {

    @Test
    public void testInit() throws Exception {

        DefaultApplierServer server = new DefaultApplierServer("ApplierTest", parser);
        server.initialize();

        assertTrue(server.sequence.getLifecycleState().isInitialized());
        assertTrue(server.lwmManager.getLifecycleState().isInitialized());
        assertTrue(server.replication.getLifecycleState().isInitialized());

        assertNotNull(server.client);
        assertNotNull(server.parser);

        assertEquals(server.client, ((DefaultLwmManager) server.lwmManager).client);
        assertEquals(server.sequence, ((DefaultLwmManager) server.lwmManager).sequence);
        assertEquals(server.lwmManager, ((DefaultSequenceController) server.sequence).lwmManager);
        assertEquals(server.parser, ((DefaultXsyncReplication) server.replication).parser);

        //server.client.close()
    }
}