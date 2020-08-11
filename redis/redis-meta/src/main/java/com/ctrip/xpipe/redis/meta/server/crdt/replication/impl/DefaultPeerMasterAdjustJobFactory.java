package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.PeerMasterAdjustJobFactory;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

@Component
public class DefaultPeerMasterAdjustJobFactory implements PeerMasterAdjustJobFactory {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DcMetaCache dcMetaCache;

    protected CurrentMetaManager currentMetaManager;

    private Executor executors;

    protected ScheduledExecutorService scheduled;

    private XpipeNettyClientKeyedObjectPool keyedObjectPool;


    @Autowired
    public DefaultPeerMasterAdjustJobFactory(DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                             @Qualifier(CLIENT_POOL) XpipeNettyClientKeyedObjectPool keyedObjectPool,
                                             @Qualifier(GLOBAL_EXECUTOR) Executor executors) {
        this.dcMetaCache = dcMetaCache;
        this.currentMetaManager = currentMetaManager;
        this.keyedObjectPool = keyedObjectPool;
        this.executors = executors;

        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("PeerMasterAdjustJobSchedule"));
    }

    public PeerMasterAdjustJob buildPeerMasterAdjustJob(String clusterId, String shardId) {
        Set<String> upstreamPeerDcs = currentMetaManager.getUpstreamPeerDcs(clusterId, shardId);
        if (upstreamPeerDcs.isEmpty()) {
            logger.info("[buildPeerMasterAdjustJob][{}][{}] unknown any upstream dcs, skip adjust", clusterId, shardId);
            return null;
        }

        RedisMeta currentMaster = currentMetaManager.getCurrentCRDTMaster(clusterId, shardId);
        if (null == currentMaster) {
            logger.info("[buildPeerMasterAdjustJob][{}][{}] unknown current master, skip adjust", clusterId, shardId);
            return null;
        }

        List<RedisMeta> allPeerMasters = currentMetaManager.getAllPeerMasters(clusterId, shardId);
        return new PeerMasterAdjustJob(clusterId, shardId, allPeerMasters,
                Pair.of(currentMaster.getIp(), currentMaster.getPort()), false,
                keyedObjectPool.getKeyPool(new DefaultEndPoint(currentMaster.getIp(), currentMaster.getPort())),
                scheduled, executors);
    }

}
