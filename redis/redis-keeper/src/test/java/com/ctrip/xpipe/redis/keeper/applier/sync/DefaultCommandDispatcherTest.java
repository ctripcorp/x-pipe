package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisSingleKeyOpGtidWrapper;
import com.ctrip.xpipe.redis.keeper.applier.threshold.GTIDDistanceThreshold;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
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
        dispatcher.gtidDistanceThreshold = new AtomicReference<>(new GTIDDistanceThreshold(2000));

        dispatcher.resetState(new GtidSet(""));
    }

    @Test
    public void testSkip() {

        dispatcher.updateGtidState("A:5");
        assertEquals(5, dispatcher.gtid_received.lwm("A"));
        assertTrue(dispatcher.receivedSids.contains("A"));

        assertTrue(dispatcher.updateGtidState("A:5"));
        assertTrue(dispatcher.updateGtidState("A:3"));
        assertFalse(dispatcher.updateGtidState("A:6"));
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

            byte[] bytes = SafeEncoder.encode(i + "");

            int rt = dispatcher.toInt(bytes);
            assertEquals(i, rt);
        }

    }

    @Test
    public void testFilterPublish() {
        RedisSingleKeyOp op1 = new RedisOpSingleKey(RedisOpType.SET, string2Bytes("set a 1"), null, null);
        RedisOp gtidOp1 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op1);
        Assert.assertFalse(dispatcher.shouldFilter(gtidOp1));

        RedisSingleKeyOp op2 = new RedisOpSingleKey(RedisOpType.PUBLISH, string2Bytes("publish xpipe-asymmetric-ppp 222"), null, null);
        RedisOp gtidOp2 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op2);
        Assert.assertFalse(dispatcher.shouldFilter(gtidOp2));

        RedisSingleKeyOp op3 = new RedisOpSingleKey(RedisOpType.PUBLISH, string2Bytes("publish ppp 222"), null, null);
        RedisOp gtidOp3 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op3);
        Assert.assertTrue(dispatcher.shouldFilter(gtidOp3));
        Assert.assertTrue(dispatcher.shouldFilter(op3));

        //test estimated size by the way
        Assert.assertEquals(15, gtidOp1.estimatedSize());
        Assert.assertEquals(40, gtidOp2.estimatedSize());
        Assert.assertEquals(23, gtidOp3.estimatedSize());
    }

    private byte[][] string2Bytes(String s) {

        String[] ss = s.split(" ");
        int length = ss.length;
        byte[][] b = new byte[length][];

        for (int i = 0; i < length; i++) {
            b[i] = ss[i].getBytes();
        }

        return b;
    }
}