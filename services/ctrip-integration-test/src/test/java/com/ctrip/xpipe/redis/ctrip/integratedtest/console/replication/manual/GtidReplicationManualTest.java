package com.ctrip.xpipe.redis.ctrip.integratedtest.console.replication.manual;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
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
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegrated;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.DefaultApplierServer;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * @author lishanglin
 * date 2022/6/12
 * 1、start XRedis 127.0.0.1:6379, 127.0.0.1:6380 with "gtid-enabled yes"
 * 2、mkdir -p /opt/config/100004374/credis
 * 3、echo 'group.master=127.0.0.1:6380' > /opt/config/100004374/credis/ApplierTest.properties
 */
public class GtidReplicationManualTest extends AbstractKeeperIntegrated implements XsyncObserver, RdbParseListener {

    private RedisKeeperServer gtidKeeperServer;

    private LeaderElectorManager leaderElectorManager;

    private RedisOpParser redisOpParser;

    private KeeperMeta keeperMeta;

    private RdbParser<?> rdbParser;

    private String reqUuid = "00229094ffbf30b0e016ccb8a9ffe327d560accc";

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
        return "gtid_repl.xml";
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

        ApplierMeta applierMeta = new ApplierMeta();
        applierMeta.setId("test-applier");
        applierMeta.setPort(7080);
        ApplierServer applierServer = new DefaultApplierServer("ApplierTest",
                ClusterId.from(1L), ShardId.from(1L), applierMeta, leaderElectorManager, redisOpParser, getKeeperConfig());

        applierServer.initialize();
        applierServer.start();

        applierServer.setStateActive(new DefaultEndPoint(keeperMeta.getIp(), keeperMeta.getPort()), new GtidSet(reqUuid + ":0"));

        waitForAnyKeyToExit();
    }

    @Override
    public void onCommand(long commandOffset, Object[] rawCmdArgs) {
        RedisOp redisOp = redisOpParser.parse(rawCmdArgs);
        logger.info("[onCommand] {}", redisOp);
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet, long rdbOffset) {
        logger.info("[onFullSync] {}", rdbGtidSet);
    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet, long rdbOffset) {
        logger.info("[beginReadRdb] {} {}", eofType, rdbGtidSet);
    }

    @Override
    public void onRdbData(ByteBuf byteBuf) {
        logger.info("[onRdbData] {}", byteBuf.readableBytes());
        rdbParser.read(byteBuf);
    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet, long rdbOffset) {
        logger.info("[endReadRdb] {} {}", eofType, rdbGtidSet);
    }

    @Override
    public void onContinue(GtidSet gtidSet, long continueOffset) {
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

    @Override
    public void onAuxFinish() {
        logger.info("[onAuxFinish]");
    }
}
