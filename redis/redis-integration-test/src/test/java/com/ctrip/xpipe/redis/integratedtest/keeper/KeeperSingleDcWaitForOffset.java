package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2016
 */
public class KeeperSingleDcWaitForOffset extends AbstractKeeperIntegratedSingleDc {

    @Test
    public void testMakeBackupActive() throws Exception {

        RedisKeeperServer redisKeeperServer = getRedisKeeperServer(backupKeeper);
        Assert.assertEquals(PARTIAL_STATE.FULL, redisKeeperServer.getRedisMaster().partialState());

        logger.info(remarkableMessage("make keeper active to wrong addr{}"), backupKeeper);

        String ip = "localhost";
        int port = randomPort();
        setKeeperState(backupKeeper, KeeperState.ACTIVE, ip, port);

        sleep(2000);

        //make sure redis has more log
        sendMessageToMaster(redisMaster, 1);

        xslaveof(backupKeeper.getIp(), backupKeeper.getPort(), getRedisSlaves());

        sleep(2000);
        setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());

        sleep(2000);
        Set<RedisSlave> slaves = redisKeeperServer.slaves();
        Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisKeeperServer.getRedisMaster().partialState());
        slaves.forEach(redisSlave -> Assert.assertEquals(PARTIAL_STATE.PARTIAL, redisSlave.partialState()));
        Assert.assertEquals(slaves.size(), redisKeeperServer.getKeeperMonitor().getKeeperStats().getWaitOffsetSucceed());
        Assert.assertEquals(0, redisKeeperServer.getKeeperMonitor().getKeeperStats().getWaitOffsetFail());

        sendMessageToMasterAndTestSlaveRedis();
    }
}
