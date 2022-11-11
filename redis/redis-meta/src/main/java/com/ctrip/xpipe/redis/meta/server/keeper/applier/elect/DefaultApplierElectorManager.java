package com.ctrip.xpipe.redis.meta.server.keeper.applier.elect;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierElectorManager;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author ayq
 * <p>
 * 2022/4/11 22:06
 */
@Component
public class DefaultApplierElectorManager extends AbstractCurrentMetaObserver implements ApplierElectorManager, Observer, TopElement {

    public static final int  FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI = 50;

    @Autowired
    private ZkClient zkClient;

    @Autowired
    private ApplierActiveElectAlgorithmManager applierActiveElectAlgorithmManager;

    private void observeLeader(final ClusterMeta cluster) {
        logger.info("[observeLeader]{}", cluster.getDbId());
        for (final ShardMeta shard : cluster.getAllShards().values()) {
            observerShardLeader(cluster.getDbId(), shard.getDbId());
        }
    }

    protected void observerShardLeader(final Long clusterDbId, final Long shardDbId) {
        logger.info("[observerShardLeader]cluster_{},shard_{}", clusterDbId, shardDbId);

        final CuratorFramework client = zkClient.get();
        if(currentMetaManager.watchApplierIfNotWatched(clusterDbId, shardDbId)){
            try {
                List<PathChildrenCache> pathChildrenCaches = new ArrayList<>();
                pathChildrenCaches.add(buildPathChildrenCacheByDbId(clusterDbId, shardDbId, client));

                ReentrantLock lock = new ReentrantLock();
                pathChildrenCaches.forEach(pathChildrenCache -> {
                    pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

                        private AtomicBoolean isFirst = new AtomicBoolean(true);
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

                            if(isFirst.get()){
                                isFirst.set(false);
                                try {
                                    logger.info("[childEvent][first sleep]{}", FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI);
                                    TimeUnit.MILLISECONDS.sleep(FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI);
                                }catch (Exception e){
                                    logger.error("[childEvent]", e);
                                }
                            }

                            try {
                                lock.lock();
                                logger.info("[childEvent]cluster_{}, shard_{}, {}, {}", clusterDbId, shardDbId, event.getType(), ZkUtils.toString(event.getData()));
                                updateShardLeader(aggregateChildData(pathChildrenCaches), clusterDbId, shardDbId);
                            } finally {
                                lock.unlock();
                            }
                        }
                    });

                    try {
                        pathChildrenCache.start();
                    } catch (Exception e) {
                        logger.info("[observerShardLeader][cluster{}][shard_{}] start cache fail {}", clusterDbId, shardDbId, pathChildrenCache, e);
                    }
                });

                registerJob(clusterDbId, shardDbId, new Releasable() {
                    @Override
                    public void release() throws Exception {
                        pathChildrenCaches.forEach(pathChildrenCache -> {
                            try{
                                logger.info("[release][release children cache]cluster_{}, shard_{}", clusterDbId, shardDbId);
                                pathChildrenCache.close();
                            }catch (Exception e){
                                logger.error(String.format("[release][release children cache]cluster_%s, shard_%s", clusterDbId, shardDbId), e);
                            }
                        });
                    }
                });
            } catch (Exception e) {
                logger.error("[observerShardLeader]cluster_" + clusterDbId + " shard_" + shardDbId, e);
            }
        }else{
            logger.warn("[observerShardLeader][already watched]cluster_{}, shard_{}", clusterDbId, shardDbId);
        }
    }

    private PathChildrenCache buildPathChildrenCacheByDbId(Long clusterDbId, Long shardDbId, CuratorFramework client) {
        final String leaderLatchPath = MetaZkConfig.getApplierLeaderLatchPath(clusterDbId, shardDbId);
        logger.info("[observerShardLeader][add PathChildrenCache]cluster_{}, shard_{}, {}", clusterDbId, shardDbId, leaderLatchPath);
        return new PathChildrenCache(client, leaderLatchPath, true,
                XpipeThreadFactory.create(String.format("ApplierPathChildrenCache:cluster_%d-shard_%d", clusterDbId, shardDbId)));
    }

    private List<List<ChildData>> aggregateChildData(List<PathChildrenCache> pathChildrenCaches) {
        List<List<ChildData>> children = new ArrayList<>();
        pathChildrenCaches.forEach(pathChildrenCache -> children.add(pathChildrenCache.getCurrentData()));
        return children;
    }

    protected void updateShardLeader(List<List<ChildData>> childrenDataList, Long clusterDbId, Long shardDbId){
        logger.info("[updateShardLeader]cluster_{}, shard_{}, {}", clusterDbId, shardDbId, childrenDataList);
        List<ApplierMeta> survivalAppliers = new ArrayList<>();
        int expectedAppliers = 0;

        for (List<ChildData> childrenData: childrenDataList) {
            expectedAppliers += childrenData.size();
            sortChildData(childrenData).forEach(childData -> {
                try {
                    String data = new String(childData.getData());
                    ApplierMeta applier = JsonCodec.INSTANCE.decode(data, ApplierMeta.class);
                    logger.info("[updateShardLeader] getApplier {}", applier);
                    survivalAppliers.add(applier);
                } catch (Throwable th) {
                    logger.info("[updateShardLeader][cluster_{},shard_{}] decode child data fail {}", clusterDbId, shardDbId, childData, th);
                }
            });
        }

        if(survivalAppliers.size() != expectedAppliers){
            throw new IllegalStateException(String.format("[children data not equal with survival appliers]%s, %s", childrenDataList, survivalAppliers));
        }

        ApplierActiveElectAlgorithm klea = applierActiveElectAlgorithmManager.get(clusterDbId, shardDbId);
        ApplierMeta activeApplier = klea.select(clusterDbId, shardDbId, survivalAppliers);

        String srcSids = currentMetaManager.getSrcSids(clusterDbId, shardDbId);
        currentMetaManager.setSurviveAppliersAndNotify(clusterDbId, shardDbId, survivalAppliers, activeApplier, srcSids);
    }

    private LockInternalsSorter sorter = new LockInternalsSorter() {
        @Override
        public String fixForSorting(String str, String lockName) {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    protected List<ChildData> sortChildData(List<ChildData> childrenData) {
        List<String> childrenPaths = new LinkedList<>();
        childrenData.forEach(childData -> childrenPaths.add(childData.getPath()));
        List<String> sortedChildren = LockInternals.getSortedChildren("latch-", sorter, childrenPaths);

        List<ChildData> sortedChildrenData = new ArrayList<>(childrenData.size());
        for(String path : sortedChildren){
            for(ChildData childData : childrenData){
                if(path.equals(childData.getPath())){
                    sortedChildrenData.add(childData);
                    break;
                }
            }
        }

        return sortedChildrenData;
    }

    @Override
    protected void handleClusterAdd(ClusterMeta clusterMeta) {
        try {
            if (DcGroupType.DR_MASTER.name().equals(clusterMeta.getDcGroupType())) {
                return;
            }

            observeLeader(clusterMeta);
        } catch (Exception e) {
            logger.error("[handleClusterAdd]cluster_" + clusterMeta.getDbId(), e);
        }
    }

    @Override
    protected void handleClusterModified(ClusterMetaComparator comparator) {
        if (DcGroupType.DR_MASTER.name().equals(comparator.getFuture().getDcGroupType())) {
            return;
        }

        Long clusterDbId = comparator.getCurrent().getDbId();
        for(ShardMeta shardMeta : comparator.getAdded()){
            try {
                observerShardLeader(clusterDbId, shardMeta.getDbId());
            } catch (Exception e) {
                logger.error("[handleClusterModified]cluster_" + clusterDbId + ",shard_" + shardMeta.getDbId(), e);
            }
        }
    }

    @Override
    protected void handleClusterDeleted(ClusterMeta clusterMeta) {
        //nothing to do
    }

    @Override
    public Set<ClusterType> getSupportClusterTypes() {
        return Collections.singleton(ClusterType.ONE_WAY);
    }

    @VisibleForTesting
    public void setApplierActiveElectAlgorithmManager(ApplierActiveElectAlgorithmManager applierActiveElectAlgorithmManager) {
        this.applierActiveElectAlgorithmManager = applierActiveElectAlgorithmManager;
    }
}
