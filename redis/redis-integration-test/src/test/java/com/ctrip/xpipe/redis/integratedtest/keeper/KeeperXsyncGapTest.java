package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SetValueCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

public class KeeperXsyncGapTest extends AbstractKeeperIntegratedSingleDc {

    private List<RedisKeeperServer> redisKeeperServers = new LinkedList<>();


    @Test
    public void testKeeperXsync() throws Exception {
        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());

        logger.info("config set gtid-enabled yes to master");

        for(int i = 0; i < 100; i++) {
            setKey("key_" + i, redisMaster.getIp(), redisMaster.getPort());
        }

        logger.info("set link start");

        initKeepers(redisMaster.getIp(), redisMaster.getPort());

        int fullSyncContInit = getFullCount();

        int redisPort = randomPort();
        logger.info("redis random port: " + redisPort);
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(redisPort);
        super.startRedis(redisMeta);
        slaveOfKeeper("127.0.0.1", redisPort);

        logger.info("set link finish redis -> keep");


        int newMasterPort = randomPort();
        RedisMeta masterMeta = new RedisMeta().setIp("127.0.0.1").setPort(newMasterPort);
        super.startRedis(masterMeta);
        setRedisToGtidEnabled(masterMeta.getIp(), masterMeta.getPort());



        setKeeperState(activeKeeper, KeeperState.ACTIVE, masterMeta.getIp(), masterMeta.getPort());
        setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());





        // 注入数据
        logger.info("send request to redis master " + fullSyncContInit);

        for(int i = 100; i < 252; i++) {
            setKey("key_" + i, redisMaster.getIp(), redisMaster.getPort());
        }



        setKeeperState(backupKeeper, KeeperState.ACTIVE, activeKeeper.getIp(), activeKeeper.getPort());
        Thread.sleep(1000);
        setKeeperState(activeKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
        Thread.sleep(1000);

        for(int i = 252; i < 274; i++) {
            setKey("key_" + i, redisMaster.getIp(), redisMaster.getPort());
        }


        Assert.assertEquals(redisKeeperServers.get(0).getReplicationStore().getGtidSet().getValue().itemCnt(), 152);
    }

    private void slaveOfKeeper(String ip, int port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        new SlaveOfCommand(keyPool, backupKeeper.getIp(), backupKeeper.getPort(), scheduled).execute().get();
    }

    private void slaveOfNoOne(String ip, int port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        new SlaveOfCommand(keyPool, null, activeKeeper.getPort(), scheduled).execute().get();
    }

    private void setKey(String key, String ip, int port) throws Exception {
        new SetValueCommand(ip, port, scheduled, key, "test_value").execute().get();

    }

    private void setRedisToGtidEnabled(String ip, Integer port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        ConfigSetCommand.ConfigSetGtidEnabled configSetGtidEnabled = new ConfigSetCommand.ConfigSetGtidEnabled(true, keyPool, scheduled);
        String gtid =  configSetGtidEnabled.execute().get().toString();
        System.out.println(gtid);
    }

    private void initKeepers(String ip, int port) throws Exception {

        redisKeeperServers.add(getRedisKeeperServer(activeKeeper));
        redisKeeperServers.add(getRedisKeeperServer(backupKeeper));
        setKeeperState(activeKeeper, KeeperState.ACTIVE, ip, port);
        setKeeperState(backupKeeper, KeeperState.ACTIVE, activeKeeper.getIp(), activeKeeper.getPort());

        waitKeepersConnected(redisKeeperServers);
    }

    private void waitKeepersConnected(List<RedisKeeperServer> keeperServers) throws Exception {
        for (RedisKeeperServer keeperServer: redisKeeperServers) {
            waitConditionUntilTimeOut(() -> keeperServer.getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }
    }

    int getFullCount() {
        int cnt = 0;
        for (RedisKeeperServer redisKeeperServer : redisKeeperServers) {
            cnt += redisKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount();
        }
        return cnt;
    }

}
