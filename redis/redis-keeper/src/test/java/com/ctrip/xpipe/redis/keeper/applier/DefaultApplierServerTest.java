package com.ctrip.xpipe.redis.keeper.applier;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 09:14
 */
public class DefaultApplierServerTest {

    @Test
    public void testInit() throws Exception {

        DefaultApplierServer server = new DefaultApplierServer("ApplierTest");
        server.initialize();

        assertTrue(server.sequence.getLifecycleState().isInitialized());
        assertTrue(server.lwmManager.getLifecycleState().isInitialized());
        assertNotNull(server.client);

        //server.client.close()
    }
}