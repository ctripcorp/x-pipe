package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.XsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author lishanglin
 * date 2022/5/25
 */
public class GtidRedisKeeperServerTest extends AbstractFakeRedisTest implements XsyncObserver {

    private FakeRedisServer fakeRedisServer;

    private RedisOpParser parser;

    private List<RedisOp> redisOps;

    @Before
    public void setupGtidRedisKeeperServerTest() throws Exception {
        fakeRedisServer = startFakeRedisServer(randomPort());
        fakeRedisServer.setCommandsLength(-1); // avoid auto generating commands
        parser = getRegistry().getComponent(GeneralRedisOpParser.class);
        redisOps = new LinkedList<>();
    }

    @After
    public void afterGtidRedisKeeperServerTest() throws Exception {
        if (null != fakeRedisServer) remove(fakeRedisServer);
    }

    @Test
    public void testPsyncAndXsync() throws Exception {
        KeeperConfig keeperConfig = newTestKeeperConfig();
        KeeperMeta keeperMeta = createKeeperMeta();
        DefaultRedisKeeperServer keeperServer = new DefaultRedisKeeperServer(getReplId().id(), keeperMeta, keeperConfig,
                getReplicationStoreManagerBaseDir(keeperMeta), getRegistry().getComponent(LeaderElectorManager.class),
                createkeepersMonitorManager(), getResourceManager(), parser);
        keeperServer.initialize();
        keeperServer.start();

        ReplicationStore replicationStore = keeperServer.getCurrentReplicationStore();
        keeperServer.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("127.0.0.1", fakeRedisServer.getPort()));

        waitConditionUntilTimeOut(() -> MASTER_STATE.REDIS_REPL_CONNECTED.equals(keeperServer.getRedisMaster().getMasterState()));

        for (int i = 1; i < 6; i++) {
            RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:"+i, "set", "k" + i, "v" + i).toArray());
            fakeRedisServer.propagate(redisOp.buildRESP().toString(Codec.defaultCharset));
        }
        waitConditionUntilTimeOut(() -> replicationStore.getEndOffset() > 50);
        logger.info("[testPsyncAndXsync] {}", replicationStore.getEndGtidSet());
        DefaultXsync xsync = new DefaultXsync("127.0.0.1", keeperServer.getListeningPort(), new GtidSet("a1:0"), null, scheduled);
        xsync.addXsyncObserver(this);
        xsync.execute(executors);

        waitConditionUntilTimeOut(() -> 5 == redisOps.size());
        Assert.assertEquals("GTID a1:1 set k1 v1", redisOps.get(0).toString());
        Assert.assertEquals("GTID a1:2 set k2 v2", redisOps.get(1).toString());
        Assert.assertEquals("GTID a1:3 set k3 v3", redisOps.get(2).toString());
        Assert.assertEquals("GTID a1:4 set k4 v4", redisOps.get(3).toString());
        Assert.assertEquals("GTID a1:5 set k5 v5", redisOps.get(4).toString());
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
