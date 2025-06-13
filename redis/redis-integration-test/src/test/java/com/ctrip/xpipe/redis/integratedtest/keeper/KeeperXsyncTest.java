package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SetValueCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

public class KeeperXsyncTest extends AbstractKeeperIntegratedSingleDc {

    private List<RedisKeeperServer> redisKeeperServers = new LinkedList<>();

    @Test
    public void testKeeperXsync() throws Exception {

        setRedisToGtidEnabled(redisMaster.getIp(), redisMaster.getPort());

        logger.info("config set gtid-enabled yes to master");

        for(int i = 0; i < 100; i++) {
            setKey("key_" + i);
        }

        logger.info("set link start");

        initKeepers();

        int fullSyncContInit = getFullCount();
        int redisPort = randomPort();
        logger.info("redis random port: " + redisPort);
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(redisPort);
        super.startRedis(redisMeta);
        slaveOfKeeper("127.0.0.1", redisPort);

        logger.info("set link finish redis -> keep");

        // 注入数据
        logger.info("send request to redis master " + fullSyncContInit);
        for(int i = 100; i < 200; i++) {
            setKey("key_" + i);
        }

        Thread.sleep(1500);

        int fullSyncConnected = getFullCount();

        // redis init, full sync
        Assert.assertEquals(fullSyncContInit + 1, fullSyncConnected);


        String masterGtid = getGtidSet(redisMaster.getIp(), redisMaster.getPort());
        String slaveGtid = getGtidSet("127.0.0.1",  redisPort);

        // gtid set (master == slave)
        Assert.assertEquals(masterGtid, slaveGtid);

        int cnt1 = getFullCount();

        redisKeeperServers.get(0).closeSlaves("test");

        Thread.sleep(1000);
        for(int i = 200; i < 300; i++) {
            setKey("key_" + i);
        }

        int cnt2 = getFullCount();

        // continue sync, will not full sync
        Assert.assertEquals(cnt2, cnt1);

        Thread.sleep(1000);
        masterGtid = getGtidSet(redisMaster.getIp(), redisMaster.getPort());
        slaveGtid = getGtidSet("127.0.0.1",  redisPort);
        Assert.assertEquals(masterGtid, slaveGtid);

        // 断keeper
        int cnt3 = getFullCount();
        for (RedisKeeperServer redisKeeperServer : redisKeeperServers) {
            redisKeeperServer.stop();
        }
        for(int i = 300; i < 400; i++) {
            setKey("key_" + i);
        }
        for (RedisKeeperServer redisKeeperServer : redisKeeperServers) {
            redisKeeperServer.start();
        }
        Thread.sleep(10000);

        masterGtid = getGtidSet(redisMaster.getIp(), redisMaster.getPort());
        slaveGtid = getGtidSet("127.0.0.1",  redisPort);
        Assert.assertEquals(masterGtid, slaveGtid);
        int cnt4 = getFullCount();

        // Assert.assertEquals(cnt4, cnt3);

        // restart redis slave will not full sync
        stopServerListeningPort(redisPort);

        for(int i = 400; i < 500; i++) {
            setKey("key_" + i);
        }

        super.startRedis(redisMeta);
        setRedisToGtidEnabled(redisMeta.getIp(), redisMeta.getPort());

        slaveOfKeeper("127.0.0.1", redisPort);

        Thread.sleep(1000);
        masterGtid = getGtidSet(redisMaster.getIp(), redisMaster.getPort());
        slaveGtid = getGtidSet("127.0.0.1",  redisPort);
        Assert.assertEquals(masterGtid, slaveGtid);

        int cnt5 = getFullCount();
        Assert.assertEquals(cnt4, cnt5);
    }

    private String getGtidSet(String ip, int port) throws Exception {
        String info = getInfo(ip, port, "gtid");
        int index = info.indexOf("gtid_set");
        int end = info.indexOf("\r\n", index);
        return info.substring(index + "gtid_set".length(), end);
    }

    int getFullCount() {
        int cnt = 0;
        logger.info("start getFullCount");
        for (RedisKeeperServer redisKeeperServer : redisKeeperServers) {
            logger.info("[full count] {}", redisKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount());
            cnt += redisKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount();
        }
        logger.info("end getFullCount");
        return cnt;
    }

    private void slaveOfKeeper(String ip, int port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        new SlaveOfCommand(keyPool, activeKeeper.getIp(), activeKeeper.getPort(), scheduled).execute().get();
    }

    private String getInfo(String ip, int port, String infokey) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        return new InfoCommand(keyPool, infokey, scheduled).execute().get();
    }

    private void setKey(String key) throws Exception {
        new SetValueCommand(redisMaster.getIp(), redisMaster.getPort(), scheduled, key, "test_value").execute().get();

    }

    private void initKeepers() throws Exception {

        redisKeeperServers.add(getRedisKeeperServer(activeKeeper));
        redisKeeperServers.add(getRedisKeeperServer(backupKeeper));
        setKeeperState(activeKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
        setKeeperState(backupKeeper, KeeperState.ACTIVE, activeKeeper.getIp(), activeKeeper.getPort());

        waitKeepersConnected(redisKeeperServers);
    }

    private void waitKeepersConnected(List<RedisKeeperServer> keeperServers) throws Exception {
        for (RedisKeeperServer keeperServer: redisKeeperServers) {
            waitConditionUntilTimeOut(() -> keeperServer.getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }
    }

    @Override
    protected void startRedis(RedisMeta redisMeta) throws IOException {

        if (redisMeta.equals(getRedisMaster())) {
            super.startRedis(redisMeta);
        } else {
            logger.info("[startRedis][do not start it]{}", redisMeta);
        }
    }

    @Override
    protected KeeperConfig getKeeperConfig() {
        return new TestKeeperConfig(1 << 20, 1 << 10, Long.MAX_VALUE, 60 * 1000);
    }

}
