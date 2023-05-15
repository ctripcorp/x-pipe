package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.XsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.server.FakeXsyncHandler;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author lishanglin
 * date 2022/2/23
 */
public class DefaultXsyncTest extends AbstractRedisOpParserTest implements XsyncObserver {

    private DefaultXsync xsync;

    private FakeXsyncServer server;

    private GtidSet gtidSet = new GtidSet("a1:1-10:15-20,b1:1-8");

    private List<RedisOp> redisOps;

    @Before
    public void setupDefaultXsyncTest() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);
        xsync = new DefaultXsync(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", server.getPort())),
                gtidSet, null, scheduled, 0);
        redisOps = new ArrayList<>();
        xsync.addXsyncObserver(this);
    }

    @Test
    public void testXsync_checkParams() throws Exception {
        AtomicReference<List<Object>> xsyncParms = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        server.setXsyncHandler(new FakeXsyncHandler() {
            @Override
            public GtidSet handleXsync(List<String> interestedSidno, GtidSet excludedGtidSet, Object excludedVectorClock) {
                xsyncParms.set(Arrays.asList(interestedSidno, excludedGtidSet, excludedVectorClock));
                latch.countDown();
                return null;
            }

            @Override
            public byte[] genRdbData() {
                return new byte[0];
            }
        });

        xsync.execute(executors);
        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));

        List<String> interestedSidno = (List<String>) xsyncParms.get().get(0);
        GtidSet excludedGtidSet = (GtidSet) xsyncParms.get().get(1);
        Object excludedVectorClock = xsyncParms.get().get(2);
        Assert.assertEquals(2, interestedSidno.size());
        Assert.assertTrue(interestedSidno.contains("a1"));
        Assert.assertTrue(interestedSidno.contains("b1"));
        Assert.assertEquals(gtidSet, excludedGtidSet);
        Assert.assertNull(excludedVectorClock);
    }

    @Test
    public void testXsyncPartialSync_checkCmds() throws Exception {
        xsync.execute(executors);
        waitConditionUntilTimeOut(() -> 1 == server.slaveCount());

        server.propagate("gtid a1:21 0 set k1 v1");
        server.propagate("gtid a1:22 0 mset k1 v1 k2 v2");
        server.propagate("gtid a1:23 0 del k1 k2");

        waitConditionUntilTimeOut(() -> 3 == redisOps.size());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("gtid", "a1:21", "0", "set", "k1", "v1")),
                redisOps.get(0).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("gtid", "a1:22", "0", "mset", "k1", "v1", "k2", "v2")),
                redisOps.get(1).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("gtid", "a1:23", "0", "del", "k1", "k2")),
                redisOps.get(2).buildRawOpArgs());
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet, long rdbOffset) {

    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet, long rdbOffset) {

    }

    @Override
    public void onRdbData(ByteBuf rdbData) {

    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet, long rdbOffset) {

    }

    @Override
    public void onContinue(GtidSet gtidSet) {

    }

    @Override
    public void onCommand(long commandOffset, Object[] rawCmdArgs) {
        RedisOp redisOp = parser.parse(rawCmdArgs);
        redisOps.add(redisOp);
    }
}
