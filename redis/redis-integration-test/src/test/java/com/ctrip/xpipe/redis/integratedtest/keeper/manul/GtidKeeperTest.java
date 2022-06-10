package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.XsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParser;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegrated;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.DefaultApplierServer;
import com.ctrip.xpipe.redis.keeper.impl.GtidRedisKeeperServer;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lishanglin
 * date 2022/5/26
 * start xredis 127.0.0.1:6379 with "gtid-enabled yes" before test
 */
public class GtidKeeperTest extends AbstractKeeperIntegrated implements XsyncObserver, RdbParseListener {

    private GtidRedisKeeperServer gtidKeeperServer;

    private LeaderElectorManager leaderElectorManager;

    private RedisOpParser redisOpParser;

    private KeeperMeta keeperMeta;

    private RdbParser<?> rdbParser;

    private String reqUuid = "1a82e1f0a716d32c34936e82df70d3dcdf50a5d8";
    //private String reqUuid = "62c5e563967583b617f0742bdbd08abc8254cd48";

    @Before
    public void setupGtidKeeperTest() throws Exception {
        startZkServer(getDcMeta("jq").getZkServer());
        keeperMeta = getKeepersBackup("jq").get(0);
        leaderElectorManager = createLeaderElectorManager(getDcMeta("jq"));
        redisOpParser = createRedisOpParser();
        gtidKeeperServer = startGtidKeeper(keeperMeta, leaderElectorManager, redisOpParser);
        rdbParser = new DefaultRdbParser();
        rdbParser.registerListener(this);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "one_keeper.xml";
    }

    @Override
    protected List<RedisMeta> getRedisSlaves() {
        return Collections.emptyList();
    }

    @Test
    public void testSyncWithGtidRedis() throws Exception {
        setKeeperState(keeperMeta, KeeperState.ACTIVE, "127.0.0.1", 6379);
        waitConditionUntilTimeOut(() -> MASTER_STATE.REDIS_REPL_CONNECTED.equals(gtidKeeperServer.getRedisMaster().getMasterState()));

        DefaultXsync xsync = new DefaultXsync(keeperMeta.getIp(), keeperMeta.getPort(), new GtidSet(reqUuid + ":0"), null, scheduled);
        xsync.addXsyncObserver(this);
        xsync.execute(executors);

//        ApplierServer server = new DefaultApplierServer(
//                "ApplierTest",
//                getXpipeNettyClientKeyedObjectPool(),
//                redisOpParser,
//                scheduled);
//
//        server.initialize();
//        server.start();
//
//        server.setState(new DefaultEndPoint(keeperMeta.getIp(), keeperMeta.getPort()), new GtidSet(reqUuid + ":0"));

        waitForAnyKeyToExit();
    }

    @Override
    public void onCommand(Object[] rawCmdArgs) {
        RedisOp redisOp = redisOpParser.parse(rawCmdArgs);
        logger.info("[onCommand] {}", redisOp);
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet) {
        logger.info("[onFullSync] {}", rdbGtidSet);
    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet) {
        logger.info("[beginReadRdb] {} {}", eofType, rdbGtidSet);
    }

    @Override
    public void onRdbData(ByteBuf byteBuf) {
        logger.info("[onRdbData] {}", byteBuf.readableBytes());
        rdbParser.read(byteBuf);
    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet) {
        logger.info("[endReadRdb] {} {}", eofType, rdbGtidSet);
    }

    @Override
    public void onContinue() {
        logger.info("[onContinue]");
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        logger.info("[onRedisOp] {}", redisOp);
    }

    @Override
    public void onAux(String key, String value) {
        logger.info("[onAux] {} {}", key, value);
    }

    @Override
    public void onFinish(RdbParser<?> parser) {
        logger.info("[onFinish] {}", parser);
    }
}
