package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.gtid.GtidSet;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import redis.clients.util.SafeEncoder;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Oct 10, 2022 15:56
 */
public class DefaultCommandDispatcherTest {

    DefaultCommandDispatcher dispatcher = new DefaultCommandDispatcher();

    @Before
    public void setUp() throws Exception {
        dispatcher.gtid_executed = new AtomicReference<>(new GtidSet(""));
        dispatcher.stateThread = MoreExecutors.newDirectExecutorService();

        dispatcher.resetGtidReceived(new GtidSet(""));
    }

    @Test
    public void testFirstReceived() {

        assertFalse(dispatcher.receivedSids.contains("A"));
        assertFalse(dispatcher.receivedSids.contains("B"));

        dispatcher.updateGtidState("A:5");
        assertEquals(5, dispatcher.gtid_received.lwm("A"));
        assertTrue(dispatcher.receivedSids.contains("A"));

        dispatcher.updateGtidState("B:10");
        assertEquals(10, dispatcher.gtid_received.lwm("B"));
        assertTrue(dispatcher.receivedSids.contains("B"));
    }

    @Test
    public void testLeap() {

        assertFalse(dispatcher.receivedSids.contains("A"));

        dispatcher.updateGtidState("A:5");
        assertEquals(new GtidSet("A:1-5"), dispatcher.gtid_executed.get());
        assertEquals(new GtidSet("A:1-5"), dispatcher.gtid_received);
        assertTrue(dispatcher.receivedSids.contains("A"));

        dispatcher.updateGtidState("A:7");

        assertEquals(new GtidSet("A:1-6"), dispatcher.gtid_executed.get());
        assertEquals(new GtidSet("A:1-7"), dispatcher.gtid_received);

        dispatcher.updateGtidState("A:9");

        assertEquals(new GtidSet("A:1-6:8"), dispatcher.gtid_executed.get());
        assertEquals(new GtidSet("A:1-9"), dispatcher.gtid_received);

    }

    @Test
    public void toInt() {
        for (int i = 0; i < 257; i++) {

            byte[] bytes = SafeEncoder.encode(i+"");

            int rt = dispatcher.toInt(bytes);
            assertEquals(i, rt);
        }

    }
}