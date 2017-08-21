package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
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
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 10, 2017
 */
@Component
public class DefaultPrimaryDcPrepareToChange implements PrimaryDcPrepareToChange{

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Resource(name = MetaServerContextConfig.CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private SentinelManager sentinelManager;

    @Override
    public void prepare(String clusterId, String shardId) {

        logger.info("[prepare]{}, {}", clusterId, shardId);

        makeMasterReadOnly(clusterId, shardId, true);

        removeSentinel(clusterId, shardId);
        
    }

    @Override
    public void deprepare(String clusterId, String shardId) {

        logger.info("[deprepare]{}, {}", clusterId, shardId);

        makeMasterReadOnly(clusterId, shardId, false);

        addSentinel(clusterId, shardId);

    }

    private void addSentinel(String clusterId, String shardId) {

        logger.info("[addSentinel]{},{}", clusterId, shardId);
        Pair<String, Integer> keeperMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);
        sentinelManager.addSentinel(clusterId, shardId, new HostPort(keeperMaster.getKey(), keeperMaster.getValue()), new ExecutionLog());

    }

    private void removeSentinel(String clusterId, String shardId) {

        logger.info("[removeSentinel]{},{}", clusterId, shardId);
        sentinelManager.removeSentinel(clusterId, shardId, new ExecutionLog());
    }

    private void makeMasterReadOnly(String clusterId, String shardId, boolean readOnly) {

        logger.info("[makeMasterReadOnly]{},{},{}", clusterId, shardId, readOnly);
        Pair<String, Integer> keeperMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);

        RedisReadonly redisReadOnly = RedisReadonly.create(keeperMaster.getKey(), keeperMaster.getValue(), keyedObjectPool, scheduled);
        try {
            if(readOnly){
                logger.info("[makeMasterReadOnly][readonly]{}", keeperMaster);
                redisReadOnly.makeReadOnly();
            }else{
                logger.info("[makeMasterReadOnly][writable]{}", keeperMaster);
                redisReadOnly.makeWritable();
            }
        } catch (Exception e) {
            logger.error("[makeMasterReadOnly]" + keeperMaster, e);
        }
    }

}
