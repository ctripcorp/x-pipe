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
        setRedisToGtidEnabled();
        initKeepers();
        int redisPort = randomPort();
        System.out.println("random port: " + redisPort);
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(redisPort);
        super.startRedis(redisMeta);
        slaveOfKeeper("127.0.0.1", redisPort);

        System.out.println("gtid : ------" + getInfo("127.0.0.1", redisPort, "gtid"));


        // Assert.assertEquals(getFullCount(), 1);

        // 注入数据

        for(int i = 0; i < 100; i++) {
            setKey("key_" + i);
        }

        Thread.sleep(1000);
        String masterGtid = getGtidSet(redisMaster.getIp(), redisMaster.getPort());
        String slaveGtid = getGtidSet("127.0.0.1",  redisPort);

        Assert.assertEquals(masterGtid, slaveGtid);


        // 增量同步

        slaveOfOnOne("127.0.0.1", redisPort);

        for(int i = 100; i < 200; i++) {
            setKey("key_" + i);
        }
        slaveOfKeeper("127.0.0.1", redisPort);
        Thread.sleep(3000);
        masterGtid = getGtidSet(redisMaster.getIp(), redisMaster.getPort());
        slaveGtid = getGtidSet("127.0.0.1",  redisPort);
        Assert.assertEquals(masterGtid, slaveGtid);

        // 断keeper
        for (RedisKeeperServer redisKeeperServer : redisKeeperServers) {
            redisKeeperServer.stop();
        }
        for(int i = 200; i < 300; i++) {
            setKey("key_" + i);
        }
        for (RedisKeeperServer redisKeeperServer : redisKeeperServers) {
            redisKeeperServer.start();
        }
        Thread.sleep(10000);

        masterGtid = getGtidSet(redisMaster.getIp(), redisMaster.getPort());
        slaveGtid = getGtidSet("127.0.0.1",  redisPort);
        Assert.assertEquals(masterGtid, slaveGtid);

    }

    private String getGtidSet(String ip, int port) throws Exception {
        String info = getInfo(ip, port, "gtid");
        int index = info.indexOf("gtid_set");
        int end = info.indexOf("\r\n", index);
        return info.substring(index + "gtid_set".length(), end);
    }

    int getFullCount() {
        int cnt = 0;
        for (RedisKeeperServer redisKeeperServer : redisKeeperServers) {
            cnt += redisKeeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount();
        }
        return cnt;
    }

    private void slaveOfKeeper(String ip, int port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        new SlaveOfCommand(keyPool, activeKeeper.getIp(), activeKeeper.getPort(), scheduled).execute().get();
    }

    private void slaveOfOnOne(String ip, int port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        new SlaveOfCommand(keyPool, null, activeKeeper.getPort(), scheduled).execute().get();
    }

    private String getInfo(String ip, int port, String infokey) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        return new InfoCommand(keyPool, infokey, scheduled).execute().get();
    }

    private void setKey(String key) throws Exception {
        new SetValueCommand(redisMaster.getIp(), redisMaster.getPort(), scheduled, key, "test_value").execute().get();

    }

    private void setRedisToGtidEnabled() throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(redisMaster.getIp(), redisMaster.getPort()));
        ConfigSetCommand.ConfigSetGtidEnabled configSetGtidEnabled = new ConfigSetCommand.ConfigSetGtidEnabled(true, keyPool, scheduled);
        String gtid =  configSetGtidEnabled.execute().get().toString();
        System.out.println(gtid);
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
