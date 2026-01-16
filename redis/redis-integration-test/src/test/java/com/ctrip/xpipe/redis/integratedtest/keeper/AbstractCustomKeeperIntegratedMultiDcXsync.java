package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class AbstractCustomKeeperIntegratedMultiDcXsync extends AbstractKeeperIntegratedMultiDcXsync{
    protected int maxGap = 0;
    @Before
    public void beforeAbstractKeeperIntegratedSingleDc() throws Exception{

        for(DcMeta dcMeta : getDcMetas()){
            startZkServer(dcMeta.getZkServer());
        }

        setKeeperActive();
        startRedises();
    }

    protected KeeperConfig getKeeperConfig(){
        KeeperConfig keeperConfig = super.getKeeperConfig();
        TestKeeperConfig testKeeperConfig = (TestKeeperConfig) keeperConfig;
        testKeeperConfig.setXsyncMaxGap(maxGap);
        return keeperConfig;
    }

    protected void setAllRedisGtidEnabled() throws Exception {
        setRedisToGtidEnabled(getRedisMaster().getIp(),getRedisMaster().getPort());
        for(RedisMeta slave:getRedisSlaves()){
            setRedisToGtidEnabled(slave.getIp(),slave.getPort());
        }
    }

    protected void setAllRedisMasterGtidEnabled() throws Exception {
        for(RedisMeta slave:getAllRedisMaster()){
            setRedisToGtidEnabled(slave.getIp(),slave.getPort());
        }
    }

    protected void checkMasterLinkStatus(String ip,int port) throws TimeoutException {
        waitConditionUntilTimeOut(() -> {
            try {
                String masterLinkStatus = infoRedis(ip,port, InfoCommand.INFO_TYPE.REPLICATION,"master_link_status");
                return masterLinkStatus.equalsIgnoreCase("up");
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);
    }

    protected void checkMasterGtidLost() throws TimeoutException {
        waitConditionUntilTimeOut(() -> {
            try {
                String masterGtidLost = getGtidSet(getRedisMaster().getIp(),getRedisMaster().getPort(),"gtid_lost");
                return !StringUtil.isEmpty(masterGtidLost);
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);
    }

    protected void checkMasterGtidLostNo() throws TimeoutException {
        waitConditionUntilTimeOut(() -> {
            try {
                String masterGtidLostStr = getGtidSet(getRedisMaster().getIp(),getRedisMaster().getPort(),"gtid_lost");
                GtidSet masterGtidLost = new GtidSet(masterGtidLostStr);
                return masterGtidLost.itemCnt() == 1;
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);
    }

    protected void checkAllMasterSlaveGtidSet() throws TimeoutException {
        waitConditionUntilTimeOut(() -> {
            try {
                String masterGtid = getGtidSet(getRedisMaster().getIp(),getRedisMaster().getPort(),"gtid_set");
                GtidSet masterGtidSet = new GtidSet(masterGtid);
                for(RedisMeta slave:getRedisSlaves()){
                    String slaveGtid = getGtidSet(slave.getIp(),slave.getPort(),"gtid_set");
                    GtidSet slaveGtidset = new GtidSet(slaveGtid);
                    if(!masterGtidSet.equals(slaveGtidset)) return false;
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);
    }

    protected void checkAllRedisMasterGtidSet() throws TimeoutException {
        waitConditionUntilTimeOut(() -> {
            try {
                String masterGtid = getGtidSet(getRedisMaster().getIp(),getRedisMaster().getPort(),"gtid_set");
                GtidSet masterGtidSet = new GtidSet(masterGtid);
                for(RedisMeta slave:getAllRedisMaster()){
                    String slaveGtid = getGtidSet(slave.getIp(),slave.getPort(),"gtid_set");
                    GtidSet slaveGtidset = new GtidSet(slaveGtid);
                    if(!masterGtidSet.equals(slaveGtidset)) return false;
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        }, 30000, 1000);
    }

    protected void checkAllMasterLinkStatus() throws TimeoutException {
        KeeperMeta backupDcKeeper = getKeeperActive("oy");

        RedisMeta backupDcSlave = getRedisSlaves("oy").get(0);
        RedisMeta activeDcSlave = getRedisSlaves("jq").get(0);

        checkMasterLinkStatus(activeDcSlave.getIp(), activeDcSlave.getPort());

        checkMasterLinkStatus(backupDcKeeper.getIp(), backupDcKeeper.getPort());

        checkMasterLinkStatus(backupDcSlave.getIp(), backupDcSlave.getPort());
    }

    protected void slaveOf(RedisMeta srcMeta,RedisMeta dstMeta) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(srcMeta.getIp(), srcMeta.getPort()));
        new SlaveOfCommand(slaveClientPool, dstMeta.getIp(), dstMeta.getPort(), scheduled).execute().get();
    }

    protected void slaveOf(RedisMeta srcMeta,KeeperMeta dstMeta) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(srcMeta.getIp(), srcMeta.getPort()));
        new SlaveOfCommand(slaveClientPool, dstMeta.getIp(), dstMeta.getPort(), scheduled).execute().get();
    }

    protected void slaveOfNoOne(RedisMeta srcMeta) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(srcMeta.getIp(), srcMeta.getPort()));
        new SlaveOfCommand(slaveClientPool, scheduled).execute().get();
    }

    protected File getRedisDataDir(RedisMeta redisMeta, File redisDir) {
        File dataDir = new File(redisDir, "data");
        return new File(dataDir,redisMeta.getPort()+"");
    }

    protected Set<String> sendIncrementMessage(int messageCount){
        return sendMasterRandomMessage(getRedisMaster(),messageCount,2);
    }

    protected void  assertSpecifiedKeyRedisEquals(RedisMeta redisMaster, List<RedisMeta> slaves, Set<String> keys){
        Map<String,String> validateResult = specifiedKeyRedisValidates(redisMaster,slaves,keys);
        Assert.assertEquals(0,validateResult.size());
    }

    protected void  assertSpecifiedKeyRedisNotEquals(RedisMeta redisMaster, List<RedisMeta> slaves, Set<String> keys){
        Map<String,String> validateResult = specifiedKeyRedisValidates(redisMaster,slaves,keys);
        Assert.assertNotEquals(0,validateResult.size());
    }

    protected Map<String,String> specifiedKeyRedisValidates(RedisMeta redisMaster, List<RedisMeta> slaves, Set<String> keys) {
        Map<String, String> values = new HashMap<>();
        Jedis jedis = createJedis(redisMaster);
        for (String key : keys) {
            values.put(key, jedis.get(key));
        }

        Map<String,String> keysWrong = new HashMap<>();

        for (RedisMeta redisSlave : slaves) {

            logger.info(remarkableMessage("[assertSpecifiedKeyRedisEquals]redisSlave:" + redisSlave));

            Jedis slave = createJedis(redisSlave);

            for (Map.Entry<String, String> entry : values.entrySet()) {
                String realValue = slave.get(entry.getKey());
                if (StringUtil.isEmpty(realValue) || !realValue.equals(entry.getValue())) {
                    keysWrong.put(entry.getKey(),slave.getClient().getHost()+":"+slave.getClient().getPort());
                }
            }

            if (keysWrong.size() != 0) {
                logger.info("[keysWrong]{}", keysWrong);
                return keysWrong;
            }
        }
        return keysWrong;
    }

    protected void sendRandomMessage(RedisMeta redisMeta, int count) {
        sendRandomMessage(redisMeta, count, 10);
    }

    protected Set<String> sendMasterRandomMessage(RedisMeta redisMeta, int count, int messageLength) {

        Jedis jedis = createJedis(redisMeta);
        logger.info("[sendRandomMessage][begin]{}", redisMeta.desc());
        Set<String> keys = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            String key = i+":-:"+redisMeta.getIp();
            sleep(10);
            jedis.set(key, randomString(messageLength));
            keys.add(key);
        }
        logger.info("[sendRandomMessage][ end ]{}", redisMeta.desc());
        return keys;
    }

    protected void sendMessage(RedisMeta redisMeta, String key,String message) {
        Jedis jedis = createJedis(redisMeta);
        logger.info("[sendMessage][{}][begin]{}", key,redisMeta.desc());
        jedis.set(key, message);
        logger.info("[sendMessage][{}][ end ]{}", key,redisMeta.desc());
    }

    protected String getXpipeMetaConfigFile() {
        return "integrated-keeper-redis-test.xml";
    }}
