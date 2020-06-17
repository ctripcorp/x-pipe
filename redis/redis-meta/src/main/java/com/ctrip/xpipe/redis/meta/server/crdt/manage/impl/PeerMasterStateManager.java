package com.ctrip.xpipe.redis.meta.server.crdt.manage.impl;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.crdt.AbstractPeerMasterMetaObserver;
import com.ctrip.xpipe.redis.meta.server.crdt.manage.PeerMasterStateAdjuster;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Component
public class PeerMasterStateManager extends AbstractPeerMasterMetaObserver {

    @Resource(name = "clientPool")
    private XpipeNettyClientKeyedObjectPool clientPool;

    @Autowired
    protected DcMetaCache dcMetaCache;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    protected ScheduledExecutorService scheduled;

    protected Map<Pair<String, String>, PeerMasterStateAdjuster> peerMasterAdjusterMap = new ConcurrentHashMap<>();

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(Math.min(OsUtils.getCpuCount(), 2), XpipeThreadFactory.create("PeerMasterStateManager"));
    }

    protected void handleClusterModified(ClusterMetaComparator comparator) {
        super.handleClusterModified(comparator);
        String clusterId = comparator.getCurrent().getId();
        for (ShardMeta shardMeta : comparator.getAdded()){
            addShard(clusterId, shardMeta.getId());
        }
        for (ShardMeta shardMeta : comparator.getRemoved()) {
            peerMasterAdjusterMap.remove(Pair.of(clusterId, shardMeta.getId()));
        }
    }

    protected void handleClusterDeleted(ClusterMeta clusterMeta) {
        String clusterId = clusterMeta.getId();
        for(ShardMeta shardMeta : clusterMeta.getShards().values()) {
            peerMasterAdjusterMap.remove(Pair.of(clusterId, shardMeta.getId()));
        }
    }

    protected void handleClusterAdd(ClusterMeta clusterMeta) {
        String clusterId = clusterMeta.getId();
        for(ShardMeta shardMeta : clusterMeta.getShards().values()){
            addShard(clusterId, shardMeta.getId());
        }
    }


    protected void handleDcsAdded(String clusterId, String shardId, Set<String> dcsAdded) {
        if (null == currentMetaManager.getPeerMaster(dcMetaCache.getCurrentDc(), clusterId, shardId)) return;

        boolean shouldAdjust = false;
        for (String dcId : dcsAdded) {
            if (null != currentMetaManager.getPeerMaster(dcId, clusterId, shardId)) {
                shouldAdjust = true;
                break;
            }
        }

        if (shouldAdjust) getOrCreateAdjuster(clusterId, shardId).adjust();
    }

    protected void handleDcsDeleted(String clusterId, String shardId, Set<String> dcsDeleted) {
        // do nothing
    }

    protected void handleRemotePeerMasterChange(String dcId, String clusterId, String shardId) {
        // do nothing
    }

    private void addShard(String clusterId, String shardId) {
        try {
            PeerMasterStateAdjuster adjuster = getOrCreateAdjuster(clusterId, shardId);
            logger.info("[getOrCreateAdjuster]{}, {}, {}", clusterId, shardId, adjuster);
            adjuster.start();
            //release resources
            currentMetaManager.addResource(clusterId, shardId, adjuster);
        } catch (Exception e) {
            logger.error("[getOrCreateAdjuster]{}, {}", clusterId, shardId, e);
        }
    }

    private PeerMasterStateAdjuster getOrCreateAdjuster(String clusterId, String shardId) {
        return MapUtils.getOrCreate(peerMasterAdjusterMap, Pair.of(clusterId, shardId), () ->
                new DefaultPeerMasterStateAdjuster(clusterId, shardId, dcMetaCache, currentMetaManager, clientPool, executors, scheduled)
        );
    }

    @VisibleForTesting
    protected void setClientPool(XpipeNettyClientKeyedObjectPool clientPool) {
        this.clientPool = clientPool;
    }

    @VisibleForTesting
    protected void setExecutor(Executor executors) {
        this.executors = executors;
    }

    @VisibleForTesting
    protected void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @VisibleForTesting
    protected Map<Pair<String, String>, PeerMasterStateAdjuster> getPeerMasterAdjusterMap() {
        return this.peerMasterAdjusterMap;
    }
}
