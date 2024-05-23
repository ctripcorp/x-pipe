package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.SyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.DefaultRunIdGenerator;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.server.FakePsyncHandler;
import com.ctrip.xpipe.redis.core.server.FakePsyncServer;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author hailu
 * @date 2024/5/9 15:08
 */
public class ApplierPsyncTest extends AbstractRedisOpParserTest implements SyncObserver {

    private ApplierPsync psync;

    private FakePsyncServer server;

    private String replId = "ddfa913a90397bb7bc1f20c12e6a6473c30190b0";

    private long offset = 666;

    private List<RedisOp> redisOps;

    @Before
    public void setupDefaultXsyncTest() throws Exception {
        server = startFakePsyncServer(randomPort(), null);
        psync = new ApplierPsync(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", server.getPort())),
                new AtomicReference<>(replId), new AtomicLong(offset), scheduled, 0);
        redisOps = new ArrayList<>();
        psync.addSyncObserver(this);
    }

    @Test
    public void testPsync_checkParams() throws Exception {
        AtomicReference<List<Object>> psyncParms = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        server.setPsyncHandler(new FakePsyncHandler() {
            @Override
            public Long handlePsync(String replId, long offset) {
                psyncParms.set(Arrays.asList(replId, offset));
                latch.countDown();
                return null;
            }

            @Override
            public byte[] genRdbData() {
                return new byte[0];
            }
        });
        psync.execute(executors);
        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        String replId = (String) psyncParms.get().get(0);
        long offset = (Long) psyncParms.get().get(1);
        Assert.assertEquals(this.replId, replId);
        Assert.assertEquals(this.offset + 1, offset);
    }
    @Test
    public void testPsync_checkFullSync() throws Exception {
        ApplierPsync psync = new ApplierPsync(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", server.getPort())),
                new AtomicReference<>("?"), new AtomicLong(-1), scheduled, 0);
        psync.addSyncObserver(this);
        AtomicReference<List<Object>> psyncParms = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        server.setPsyncHandler(new FakePsyncHandler() {
            @Override
            public Long handlePsync(String replId, long offset) {
                if (replId.equals("?")) {
                    offset = 999;
                    replId = DefaultRunIdGenerator.DEFAULT.generateRunid();
                }
                psyncParms.set(Arrays.asList(replId, offset));
                latch.countDown();
                return offset;
            }

            @Override
            public byte[] genRdbData() {
                return new byte[0];
            }
        });
        psync.execute(executors);
        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        String replId = (String) psyncParms.get().get(0);
        long offset = (Long) psyncParms.get().get(1);
        Assert.assertEquals(RedisProtocol.RUN_ID_LENGTH, replId.length());
        Assert.assertEquals(999, offset);
    }


    @Test
    public void testPsyncPartialSync_checkCmds() throws Exception {
        psync.execute();
        waitConditionUntilTimeOut(() -> server.slaveCount() == 1);
        server.propagate("set k1 v1");
        server.propagate("mset k1 v1 k2 v2");
        server.propagate("del k1 k2");

        waitConditionUntilTimeOut(() -> redisOps.size() == 3);

        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("set", "k1", "v1")),
                redisOps.get(0).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("mset", "k1", "v1", "k2", "v2")),
                redisOps.get(1).buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("del", "k1", "k2")),
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
    public void onContinue(GtidSet gtidSet, long continueOffset) {

    }

    @Override
    public void onCommand(long commandOffset, Object[] rawCmdArgs) {
        RedisOp redisOp = parser.parse(rawCmdArgs);
        redisOps.add(redisOp);
    }
}
