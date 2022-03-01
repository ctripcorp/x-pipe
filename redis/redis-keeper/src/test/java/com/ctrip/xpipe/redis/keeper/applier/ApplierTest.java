package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.XsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import com.ctrip.xpipe.redis.keeper.applier.command.ApplierRedisOpCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultApplierCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Slight
 * <p>
 * Mar 01, 2022 8:11 AM
 */
public class ApplierTest extends AbstractRedisOpParserTest implements XsyncObserver {

    private DefaultXsync xsync;

    private FakeXsyncServer server;

    private GtidSet gtidSet = new GtidSet("a1:1-10:15-20,b1:1-8");

    private List<RedisOp> redisOps;

    private ApplierSequenceController sequenceController;

    private AsyncRedisClient client;

    @Before
    public void setup() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);
        xsync = new DefaultXsync(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", server.getPort())),
                gtidSet, null, scheduled);
        redisOps = new ArrayList<>();
        xsync.addXsyncObserver(this);

        client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient("ApplierTest");
        sequenceController = new DefaultSequenceController();
        sequenceController.initialize();
    }

    @After
    public void tearDown() throws Exception {
        sequenceController.dispose();
    }

    @Test
    public void secondHalf() throws TimeoutException {
        xsync.execute(executors);
        waitConditionUntilTimeOut(() -> 1 == server.slaveCount());

        server.propagate("gtid a1:21 set k1 v1");
        server.propagate("gtid a1:22 mset k1 v1 k2 v2");
        server.propagate("gtid a1:23 del k1 k2");
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet) {
        
    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet) {

    }

    @Override
    public void onRdbData(Object rdbData) {

    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet) {

    }

    @Override
    public void onContinue() {

    }

    @Override
    public void onCommand(Object[] rawCmdArgs) {
        RedisOp redisOp = parser.parse(Stream.of(rawCmdArgs).map(Object::toString).collect(Collectors.toList()));
        ApplierRedisOpCommand<Boolean> command = new DefaultApplierCommand(client, redisOp);
        command.sharding().forEach(sequenceController::submit);
    }
}
