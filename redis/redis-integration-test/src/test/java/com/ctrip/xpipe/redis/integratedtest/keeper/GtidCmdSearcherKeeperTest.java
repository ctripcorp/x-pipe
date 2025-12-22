package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.keeper.store.searcher.CmdKeyItem;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;

public class GtidCmdSearcherKeeperTest extends AbstractKeeperIntegratedSingleDc{

    @Test
    public void testCmdSearcher() throws Exception {
        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());
        setKeeperState(activeKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());

        waitForKeeperSync(activeKeeper);

        jedisExecCommand(redisMaster.getIp(), redisMaster.getPort(), "SET", "K1", "V1");
        jedisExecCommand(redisMaster.getIp(), redisMaster.getPort(), "MSET", "K2", "V2", "K3", "V3");
        try (Jedis jedis = new Jedis(redisMaster.getIp(), redisMaster.getPort())) {
            Transaction transaction = jedis.multi();
            transaction.set("K4", "V4");
            transaction.mset("K5", "V5", "K6", "V6");
            transaction.exec();
        }

        String raw_gtid = infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.GTID, "gtid_executed");
        String[] raw = raw_gtid.split(":");
        String[] raw_gno = raw[1].split("-");
        String UUID = raw[0];
        int begGno = Integer.parseInt(raw_gno[0]);
        int endGno = Integer.parseInt(raw_gno[1]);
        Assert.assertEquals(1, begGno);
        Assert.assertEquals(3, endGno);

        waitForKeeperSync(activeKeeper);

        List<CmdKeyItem> items = getRedisKeeperServer(activeKeeper).createCmdKeySearcher(UUID, begGno, endGno).execute().get();
        Assert.assertEquals(6, items.size());
        for (CmdKeyItem item : items) {
            Assert.assertEquals(item.uuid, UUID);
            Assert.assertTrue(item.seq >= begGno && item.seq <= endGno);
            Assert.assertEquals(0, item.dbId);
            Assert.assertNotNull(item.key);
        }
    }

    private void waitForKeeperSync(KeeperMeta keeper) throws Exception {
        waitConditionUntilTimeOut(() -> {
            long masterReplOff;
            long keeperReplOff;
            try {
                masterReplOff = Long.parseLong(infoRedis(redisMaster.getIp(), redisMaster.getPort(), InfoCommand.INFO_TYPE.REPLICATION, "master_repl_offset"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            keeperReplOff = getRedisKeeperServer(keeper).getReplicationStore().getCurReplStageReplOff();
            return masterReplOff == keeperReplOff;
        });
    }

}
