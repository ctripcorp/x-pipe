package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoReplicationComplementCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.PrimaryDcPrepareToChange;
import com.ctrip.xpipe.redis.meta.server.dcchange.RedisReadonly;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 10, 2017
 */
@Component
public class DefaultPrimaryDcPrepareToChange implements PrimaryDcPrepareToChange{

    private Logger logger = LoggerFactory.getLogger(getClass());

    private  int waitForMasterInfoMilli = 2000;

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Resource(name = MetaServerContextConfig.CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private SentinelManager sentinelManager;

    @Autowired
    private CurrentClusterServer currentClusterServer;

    @Override
    public MetaServerConsoleService.PreviousPrimaryDcMessage prepare(String clusterId, String shardId) {

        logger.info("[prepare]{}, {}", clusterId, shardId);

        MetaServerConsoleService.PreviousPrimaryDcMessage message = new MetaServerConsoleService.PreviousPrimaryDcMessage();
        ExecutionLog executionLog = new ExecutionLog(String.format("meta server:%s", currentClusterServer.getClusterInfo()));

        Pair<String, Integer> keeperMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);
        message.setMasterAddr(new HostPort(keeperMaster.getKey(), keeperMaster.getValue()));
        executionLog.info("[prepare]" + keeperMaster);

        // no need to deal with Redis 2.x whose master_repl_offset will be reset after master changed
        // since all redis version GET 4.x in Ctrip
        makeMasterReadOnly(clusterId, shardId, keeperMaster, true, executionLog);

        RedisInfo redisInfo = getInfoReplication(keeperMaster, executionLog);
        MasterInfo masterInfo = convert(redisInfo, executionLog);
        message.setMasterInfo(masterInfo);

        logger.info("[prepare]{}, {}, {}", keeperMaster, redisInfo, masterInfo);

        removeSentinel(clusterId, shardId, executionLog);

        message.setMessage(executionLog.getLog());
        return message;
        
    }

    @Override
    public MetaServerConsoleService.PreviousPrimaryDcMessage deprepare(String clusterId, String shardId) {

        logger.info("[deprepare]{}, {}", clusterId, shardId);

        MetaServerConsoleService.PreviousPrimaryDcMessage message = new MetaServerConsoleService.PreviousPrimaryDcMessage();
        ExecutionLog executionLog = new ExecutionLog(String.format("meta server:%s", currentClusterServer.getClusterInfo()));

        Pair<String, Integer> keeperMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);
        message.setMasterAddr(new HostPort(keeperMaster.getKey(), keeperMaster.getValue()));
        executionLog.info("[deprepare]" + keeperMaster);

        makeMasterReadOnly(clusterId, shardId, keeperMaster, false, executionLog);

        addSentinel(clusterId, shardId, executionLog);

        message.setMessage(executionLog.getLog());
        return message;
    }

    private void addSentinel(String clusterId, String shardId, ExecutionLog executionLog) {

        logger.info("[addSentinel]{},{}", clusterId, shardId);
        Pair<String, Integer> keeperMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);
        sentinelManager.addSentinel(clusterId, shardId, new HostPort(keeperMaster.getKey(), keeperMaster.getValue()), executionLog);

    }

    private void removeSentinel(String clusterId, String shardId, ExecutionLog executionLog) {

        logger.info("[removeSentinel]{},{}", clusterId, shardId);
        sentinelManager.removeSentinel(clusterId, shardId, executionLog);
    }

    private Pair<String, Integer> makeMasterReadOnly(String clusterId, String shardId, Pair<String, Integer> keeperMaster, boolean readOnly, ExecutionLog executionLog) {

        logger.info("[makeMasterReadOnly]{},{},{}", clusterId, shardId, readOnly);

        RedisReadonly redisReadOnly = RedisReadonly.create(keeperMaster.getKey(), keeperMaster.getValue(), keyedObjectPool, scheduled);
        try {
            executionLog.info(String.format("[makeMasterReadOnly][begin] %s:%s", keeperMaster, readOnly));
            if(readOnly){
                logger.info("[makeMasterReadOnly][readonly]{}", keeperMaster);
                redisReadOnly.makeReadOnly();
            }else{
                logger.info("[makeMasterReadOnly][writable]{}", keeperMaster);
                redisReadOnly.makeWritable();
            }
        } catch (Exception e) {
            logger.error("[makeMasterReadOnly]" + keeperMaster, e);
            executionLog.error(e.getMessage());
        }
        return keeperMaster;
    }

    public RedisInfo getInfoReplication(Pair<String, Integer> redisMaster, ExecutionLog executionLog) {

        InfoReplicationComplementCommand command = new InfoReplicationComplementCommand(
                keyedObjectPool.getKeyPool(new DefaultEndPoint(redisMaster.getKey(), redisMaster.getValue())),
                scheduled
        );


        try {
            executionLog.info("[getInfoReplication]" + redisMaster);
            RedisInfo redisInfo = command.execute().get(waitForMasterInfoMilli, TimeUnit.MILLISECONDS);
            executionLog.info("[getInfoReplication]" + redisInfo);
            return redisInfo;
        } catch (InterruptedException e) {
            logger.error("[getInfoReplication]" + redisMaster, e);
            executionLog.error(e.getMessage());
        } catch (ExecutionException e) {
            logger.error("[getInfoReplication]" + redisMaster, e);
            executionLog.error(e.getMessage());
        } catch (TimeoutException e) {
            logger.error("[getInfoReplication]" + redisMaster, e);
            executionLog.error(e.getMessage());
        }
        return null;
    }

    private MasterInfo convert(RedisInfo redisInfo, ExecutionLog executionLog) {

        if(redisInfo == null){
            return null;
        }

        if(redisInfo instanceof MasterInfo){
            return (MasterInfo) redisInfo;
        }

        if(redisInfo instanceof SlaveInfo){
            executionLog.info("[convert][SlaveInfo]" + redisInfo);
            return ((SlaveInfo) redisInfo).toMasterInfo();
        }

        throw new IllegalStateException("unknown redisInfo:" + redisInfo);
    }


}
