package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import org.junit.Assert;

import java.util.concurrent.ExecutionException;

public class AbstractKeeperIntegratedMultiDcXsync extends AbstractKeeperIntegratedMultiDc {

    protected void setRedisToGtidEnabled(String ip, Integer port) throws Exception {
        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(ip, port));
        ConfigSetCommand.ConfigSetGtidEnabled configSetGtidEnabled = new ConfigSetCommand.ConfigSetGtidEnabled(true, keyPool, scheduled);
        String gtid = configSetGtidEnabled.execute().get().toString();
        System.out.println(gtid);
    }

    protected String getGtidSet(String ip, int port, String key) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(ip, port));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.GTID, scheduled);
        String value = infoCommand.execute().get();
        logger.info("get gtid set from {}, {}, {}", ip, port, value);
        String gtidSet = new InfoResultExtractor(value).extract(key);
        return gtidSet;
    }

    protected void assertGtid(RedisMeta master) throws ExecutionException, InterruptedException {
        String masterGtid = getGtidSet(master.getIp(), master.getPort(), "gtid_set");
        String activeKeeperGtid = getGtidSet(getKeeperActive(getPrimaryDc()).getIp(), getKeeperActive(getPrimaryDc()).getPort(), "gtid_executed");
        String backGtidSet = getGtidSet(getKeeperActive(getBackupDc()).getIp(), getKeeperActive(getBackupDc()).getPort(), "gtid_executed");
        getOffset(getKeeperActive(getPrimaryDc()).getIp(), getKeeperActive(getPrimaryDc()).getPort(), true);
        logger.info("masterGtid:{}", masterGtid);
        logger.info("activeKeeperGtid:{}", activeKeeperGtid);
        logger.info("backGtidSet:{}", backGtidSet);
        for(RedisMeta slave: getRedisSlaves()) {
            String slaveGtidStr = getGtidSet(slave.getIp(), slave.getPort(), "gtid_set");
            logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveGtidStr);
            Assert.assertEquals(new GtidSet(masterGtid), new GtidSet(slaveGtidStr));
        }

    }

    protected Long getOffset(String ip, int port, boolean master) throws ExecutionException, InterruptedException {
        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(ip, port));
        InfoCommand infoCommand = new InfoCommand(masterClientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
        String value = infoCommand.execute().get();
        logger.info("get gtid set from {}, {}, {}", ip, port, value);
        String gtidSet;
        if(master) {
            gtidSet = new InfoResultExtractor(value).extract("master_repl_offset");
        } else {
            gtidSet = new InfoResultExtractor(value).extract("slave_repl_offset");
        }
        return Long.parseLong(gtidSet);
    }

    protected void assertReplOffset(RedisMeta master) throws Exception {
        long masterOffset = getOffset(master.getIp(), master.getPort(), true);
        for(RedisMeta slave: getRedisSlaves()) {
            long slaveOffset = getOffset(slave.getIp(), slave.getPort(), false);
            logger.info("slave {}:{} gtid set: {}", slave.getIp(), slave.getPort(), slaveOffset);
            Assert.assertEquals(masterOffset, slaveOffset);
        }
    }

    protected void setRedisMaster(RedisMeta redis, HostPort redisMaster) throws Exception {
        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(redis.getIp(), redis.getPort()));
        new SlaveOfCommand(slaveClientPool, redisMaster.getHost(), redisMaster.getPort(), scheduled).execute().get();
    }
}
