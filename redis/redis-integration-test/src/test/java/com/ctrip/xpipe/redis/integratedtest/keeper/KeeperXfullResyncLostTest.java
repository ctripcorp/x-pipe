package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.SetValueCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

public class KeeperXfullResyncLostTest extends AbstractKeeperIntegratedSingleDc {

    private final List<RedisKeeperServer> redisKeeperServers = new LinkedList<>();

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig config = (TestKeeperConfig) super.getKeeperConfig();
        config.setXsyncMaxGap(10000);
        config.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(1024 * 1024);
        return config;
    }

    @Test
    public void testXfullResyncSlaveLostEmptyWhenRdbCoversKeeperLost() throws Exception {
        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());

        for (int i = 0; i < 200; i++) {
            setKey("key_" + i, redisMaster.getIp(), redisMaster.getPort());
        }

        initKeepers(redisMaster.getIp(), redisMaster.getPort());
        sleep(1500);

        RedisKeeperServer activeKeeperServer = redisKeeperServers.get(0);
        waitConditionUntilTimeOut(() ->
                activeKeeperServer.getKeeperRepl().currentStage().getProto() == ReplStage.ReplProto.XSYNC);

        // XSYNC 增量同步不会自动落盘 RDB；挂一个 Redis slave 触发 Keeper 从 master dump RDB
        int rdbTriggerPort = randomPort();
        RedisMeta rdbTriggerRedis = new RedisMeta().setIp("127.0.0.1").setPort(rdbTriggerPort);
        super.startRedis(rdbTriggerRedis);
        slaveOfKeeper(rdbTriggerRedis.getIp(), rdbTriggerRedis.getPort());
        waitSlaveOnline(rdbTriggerRedis.getIp(), rdbTriggerRedis.getPort());

        waitConditionUntilTimeOut(() -> {
            RdbStore rs = ((DefaultReplicationStore) activeKeeperServer.getReplicationStore()).getRdbStore();
            return rs != null && rs.supportGtidSet();
        }, 60000, 1000);

        RdbStore rdbStore = ((DefaultReplicationStore) activeKeeperServer.getReplicationStore()).getRdbStore();
        Assert.assertNotNull(rdbStore);
        GtidSet rdbGtidExecuted = new GtidSet(rdbStore.getGtidSet());
        Assert.assertFalse(rdbGtidExecuted.isEmpty());

        GtidSet.UUIDSet firstUuidSet = rdbGtidExecuted.getUUIDSets().iterator().next();
        long endGno = firstUuidSet.getIntervals().get(0).getEnd();
        Assert.assertTrue(endGno >= 10);
        GtidSet artificialLost = new GtidSet(firstUuidSet.getUUID() + ":1-10");

        MetaStore metaStore = activeKeeperServer.getReplicationStore().getMetaStore();
        Assert.assertTrue(metaStore.increaseLost(artificialLost));

        GtidSet keeperLost = activeKeeperServer.getReplicationStore().getGtidSet().getValue();
        logger.info("[testXfullResyncSlaveLostEmptyWhenRdbCoversKeeperLost][keeperLost] {}", keeperLost);
        logger.info("[testXfullResyncSlaveLostEmptyWhenRdbCoversKeeperLost][rdbGtidExecuted] {}", rdbGtidExecuted);
        Assert.assertTrue(keeperLost.itemCnt() > 0);
        Assert.assertTrue(keeperLost.subtract(rdbGtidExecuted).isEmpty());

        int redisPort = randomPort();
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(redisPort);
        super.startRedis(redisMeta);
        setRedisToGtidEnabled(redisMeta.getIp(), redisMeta.getPort());
        setRedisToGtidMaxGap(redisMeta.getIp(), redisMeta.getPort(), 10000);

        xslaveof(activeKeeper.getIp(), activeKeeper.getPort(), redisMeta);
        waitSlaveOnline(redisMeta.getIp(), redisMeta.getPort());

        waitConditionUntilTimeOut(() -> {
            try {
                String slaveLost = getGtidSet(redisMeta.getIp(), redisMeta.getPort(), "gtid_lost");
                logger.info("[testXfullResyncSlaveLostEmptyWhenRdbCoversKeeperLost][slaveLost] {}", slaveLost);
                return new GtidSet(slaveLost).isEmpty();
            } catch (Exception e) {
                logger.info("[testXfullResyncSlaveLostEmptyWhenRdbCoversKeeperLost][wait slave lost empty]", e);
                return false;
            }
        }, 180000, 3000);

        String slaveLost = getGtidSet(redisMeta.getIp(), redisMeta.getPort(), "gtid_lost");
        String keeperExecuted = activeKeeperServer.getReplicationStore().getGtidSet().getKey().toString();
        String slaveExecuted = getGtidSet(redisMeta.getIp(), redisMeta.getPort(), "gtid_executed");

        logger.info("[testXfullResyncSlaveLostEmptyWhenRdbCoversKeeperLost][keeperExecuted] {}", keeperExecuted);
        logger.info("[testXfullResyncSlaveLostEmptyWhenRdbCoversKeeperLost][slaveExecuted] {}", slaveExecuted);

        Assert.assertTrue(new GtidSet(slaveLost).isEmpty());
        Assert.assertEquals(new GtidSet(keeperExecuted), new GtidSet(slaveExecuted));
    }

    private void slaveOfKeeper(String ip, int port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint(ip, port));
        new SlaveOfCommand(keyPool, activeKeeper.getIp(), activeKeeper.getPort(), scheduled).execute().get();
    }

    private void setKey(String key, String ip, int port) throws Exception {
        new SetValueCommand(ip, port, scheduled, key, "test_value").execute().get();
    }

    private void initKeepers(String ip, int port) throws Exception {
        redisKeeperServers.add(getRedisKeeperServer(activeKeeper));
        redisKeeperServers.add(getRedisKeeperServer(backupKeeper));
        setKeeperState(activeKeeper, KeeperState.ACTIVE, ip, port);
        setKeeperState(backupKeeper, KeeperState.ACTIVE, activeKeeper.getIp(), activeKeeper.getPort());
        waitKeepersConnected();
    }

    private void waitKeepersConnected() throws Exception {
        for (RedisKeeperServer keeperServer : redisKeeperServers) {
            waitConditionUntilTimeOut(() -> keeperServer.getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }
    }
}
