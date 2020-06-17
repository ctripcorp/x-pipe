package com.ctrip.xpipe.redis.meta.server.crdt.peermaster.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.crdt.AbstractPeerMasterMetaObserver;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Component
public class PeerMasterChooserManager extends AbstractPeerMasterMetaObserver {

    @Resource(name = "clientPool")
    private XpipeNettyClientKeyedObjectPool clientPool;

    @Autowired
    protected DcMetaCache dcMetaCache;

    @Autowired
    private MultiDcService multiDcService;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    protected ScheduledExecutorService scheduled;

    protected Map<Pair<String, String>, PeerMasterChooser> peerMasterChooserMap = new ConcurrentHashMap<>();

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(Math.min(OsUtils.getCpuCount(), 2), XpipeThreadFactory.create("PeerMasterChooserSchedule"));
    }

    @Override
    public Set<ClusterType> getSupportClusterTypes() {
        return Collections.singleton(ClusterType.BI_DIRECTION);
    }

    @Override
    protected void handleClusterModified(ClusterMetaComparator comparator) {
        super.handleClusterModified(comparator);
        String clusterId = comparator.getCurrent().getId();
        for(ShardMeta shardMeta : comparator.getAdded()){
            addShard(clusterId, shardMeta.getId());
        }
        for (ShardMeta shardMeta : comparator.getRemoved()) {
            peerMasterChooserMap.remove(Pair.of(clusterId, shardMeta.getId()));
        }
    }

    @Override
    protected void handleClusterDeleted(ClusterMeta clusterMeta) {
        String clusterId = clusterMeta.getId();
        for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
            peerMasterChooserMap.remove(Pair.of(clusterId, shardMeta.getId()));
        }
    }

    @Override
    protected void handleClusterAdd(ClusterMeta clusterMeta) {
        String clusterId = clusterMeta.getId();
        for(ShardMeta shardMeta : clusterMeta.getShards().values()){
            addShard(clusterId, shardMeta.getId());
        }
    }

    private void addShard(String clusterId, String shardId) {
        try {
            PeerMasterChooser chooser = getOrCreatePeerMasterChooser(clusterId, shardId);
            logger.info("[getOrCreatePeerMasterChooser]{}, {}, {}", clusterId, shardId, chooser);
            chooser.start();
            //release resources
            currentMetaManager.addResource(clusterId, shardId, chooser);
        } catch (Exception e) {
            logger.error("[getOrCreatePeerMasterChooser]{}, {}", clusterId, shardId);
        }
    }

    private PeerMasterChooser getOrCreatePeerMasterChooser(String clusterId, String shardId) {
        return MapUtils.getOrCreate(peerMasterChooserMap, Pair.of(clusterId, shardId), () ->
                new DefaultPeerMasterChooser(clusterId, shardId, dcMetaCache, currentMetaManager,
                        clientPool, multiDcService, executors, scheduled)
        );
    }

    protected void handleDcsAdded(String clusterId, String shardId, Set<String> dcsAdded) {
        // do nothing
    }

    protected void handleDcsDeleted(String clusterId, String shardId, Set<String> dcsDeleted) {
        dcsDeleted.forEach(dcId -> currentMetaManager.removePeerMaster(dcId, clusterId, shardId));
    }

    protected void handleRemotePeerMasterChange(String dcId, String clusterId, String shardId) {
        getOrCreatePeerMasterChooser(clusterId, shardId).createMasterChooserCommand(dcId).execute(executors);
    }

    @VisibleForTesting
    protected Map<Pair<String, String>, PeerMasterChooser> getPeerMasterChooserMap() {
        return peerMasterChooserMap;
    }

}
