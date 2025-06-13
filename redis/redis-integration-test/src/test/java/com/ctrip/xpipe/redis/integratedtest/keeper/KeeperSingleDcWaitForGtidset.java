package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class KeeperSingleDcWaitForGtidset extends AbstractKeeperIntegratedSingleDc {

    @Test
    public void testMakeBackupActive() throws Exception {

        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());
        Assert.assertTrue(isRedisGtidEnabled(redisMaster.getIp(), redisMaster.getPort()));

        RedisKeeperServer redisKeeperServer = getRedisKeeperServer(backupKeeper);
        Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
        // wait keeper repl stage switch
        waitConditionUntilTimeOut(() -> redisKeeperServer.getKeeperRepl().currentStage().getProto().equals(ReplStage.ReplProto.XSYNC));

        logger.info(remarkableMessage("make keeper active to wrong addr{}"), backupKeeper);
        String ip = "localhost";
        int port = randomPort();
        setKeeperState(backupKeeper, KeeperState.ACTIVE, ip, port);

        sleep(2000);

        //make sure redis has more log
        sendMessageToMaster(redisMaster, 1);

        logger.info("make slaves slave of keeper {}:{}", backupKeeper.getIp(), backupKeeper.getPort());
        xslaveof(backupKeeper.getIp(), backupKeeper.getPort(), slaves);

        sleep(2000);
        setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());

        sleep(2000);
        logger.info("keeper current: {}", redisKeeperServer.getKeeperRepl().currentStage());
        
        Set<RedisSlave> slaves = redisKeeperServer.slaves();
        Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
        slaves.forEach(redisSlave -> Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisSlave.partialState()));
        Assert.assertEquals(slaves.size(), redisKeeperServer.getKeeperMonitor().getKeeperStats().getWaitOffsetSucceed());
        Assert.assertEquals(0, redisKeeperServer.getKeeperMonitor().getKeeperStats().getWaitOffsetFail());

        sendMessageToMasterAndTestSlaveRedis();
    }

}
