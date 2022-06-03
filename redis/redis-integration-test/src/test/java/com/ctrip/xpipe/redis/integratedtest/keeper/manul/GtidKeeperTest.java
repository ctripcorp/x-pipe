package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
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
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegrated;
import com.ctrip.xpipe.redis.keeper.impl.GtidRedisKeeperServer;
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
public class GtidKeeperTest extends AbstractKeeperIntegrated implements XsyncObserver {

    private GtidRedisKeeperServer gtidKeeperServer;

    private LeaderElectorManager leaderElectorManager;

    private RedisOpParser redisOpParser;

    private KeeperMeta keeperMeta;

    @Before
    public void setupGtidKeeperTest() throws Exception {
        startZkServer(getDcMeta("jq").getZkServer());
        keeperMeta = getKeepersBackup("jq").get(0);
        leaderElectorManager = createLeaderElectorManager(getDcMeta("jq"));
        redisOpParser = createRedisOpParser();
        gtidKeeperServer = startGtidKeeper(keeperMeta, leaderElectorManager, redisOpParser);
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

        DefaultXsync xsync = new DefaultXsync(keeperMeta.getIp(), keeperMeta.getPort(), new GtidSet("a1:0"), null, scheduled);
        xsync.addXsyncObserver(this);
        xsync.execute(executors);

        waitForAnyKeyToExit();
    }

    @Override
    public void onCommand(Object[] rawCmdArgs) {
        RedisOp redisOp = redisOpParser.parse(rawCmdArgs);
        logger.info("[onCommand] {}", redisOp);
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

}
