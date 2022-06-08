package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.parser.*;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Slight
 * <p>
 * May 29, 2022 17:28
 */
public class SequenceControllerToGtidKeeperTest extends GtidKeeperTest {

    protected RedisOpParserManager redisOpParserManager;

    protected RedisOpParser parser;

    private DefaultXsync xsync;

    private FakeXsyncServer server;

    private GtidSet gtidSet = new GtidSet("a1:1-10:15-20,b1:1-8");

    private List<RedisOp> redisOps;

    private ApplierSequenceController sequenceController;

    private AsyncRedisClient client;

    @Before
    public void setup() throws Exception {

        redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        parser = new GeneralRedisOpParser(redisOpParserManager);

        server = startFakeXsyncServer(randomPort(), null);
        xsync = new DefaultXsync(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", server.getPort())),
                gtidSet, null, scheduled);
        redisOps = new ArrayList<>();
        xsync.addXsyncObserver(this);

        //USE CREDIS
        //client = new CRedisAsyncClientFactory().getOrCreateClient("ApplierTest");

        client = AsyncRedisClientFactory.DEFAULT.getOrCreateClient("ApplierTest");
        sequenceController = new DefaultSequenceController();
        sequenceController.initialize();
    }

    @After
    public void tearDown() throws Exception {
        sequenceController.dispose();
    }

    private boolean inTransaction = false;

    @Override
    public void onCommand(Object[] rawCmdArgs) {
        RedisOp redisOp = parser.parse(rawCmdArgs);
        if (redisOp.getOpType().equals(RedisOpType.PING) || redisOp.getOpType().equals(RedisOpType.SELECT)) {
            return;
        }
        RedisOpDataCommand<Boolean> command = new DefaultDataCommand(client, redisOp);
        switch (command.type()) {
            case MULTI:
                inTransaction = true;
            case EXEC:
                inTransaction = false;
            default:
                sequenceController.submit(command);
        }
    }
}
