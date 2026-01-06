package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class KeeperCmdRotateTest extends AbstractKeeperIntegratedMultiDcXsync {

    protected KeeperConfig getKeeperConfig() {
        return new TestKeeperConfig(1024, 400,
                102400, 60000);
    }

    @Test
    public void testRotate() throws Exception {
        RedisMeta master = getRedisMaster();
        List<RedisMeta> slaves = getRedises(getBackupDc());

        while(true) {
            logger.info("[testRotate] begin");
            sendMessagesToMasterAndTestSlaves(1000, master, slaves);
            sleep(100);
        }
    }

    private void sendMessagesToMasterAndTestSlaves(int cnt, RedisMeta master, List<RedisMeta> slaves) throws Exception {
        List<Jedis> jedisList = new ArrayList<>(slaves.size() + 1);
        jedisList.add(createJedis(master));
        for (RedisMeta redisMeta : slaves) {
            jedisList.add(createJedis(redisMeta));
        }

        String value = generateRandomString(256);
        for (int i = 0; i < cnt; i++) {
            jedisList.get(0).set("key_" + i, value);
        }

        long masterOffset = Long.parseLong(infoRedis(master.getIp(), master.getPort(), InfoCommand.INFO_TYPE.REPLICATION, "master_repl_offset"));
        waitConditionUntilTimeOut(() -> {
            try {
                for (RedisMeta redisMeta : slaves) {
                    long slaveOffset = Long.parseLong(infoRedis(redisMeta.getIp(), redisMeta.getPort(), InfoCommand.INFO_TYPE.REPLICATION, "master_repl_offset"));
                    if (slaveOffset < masterOffset) return false;
                }
            } catch (Exception e) {
                return false;
            }
            return true;
        }, 30000, 100);

        for (int i = 1; i < jedisList.size(); i++) {
            Jedis jedis = jedisList.get(i);
            for (int j = 0; j < cnt; j++) {
                Assert.assertEquals(value, jedis.get("key_" + j));
            }
        }

        for (Jedis jedis : jedisList) {
            jedis.close();
        }
    }

}
