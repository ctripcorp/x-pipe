package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.InstanceNode;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.RedisGtidCollector;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.RedisGtidCollectorManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/8/22 15:17
 */

@Component
public class DefaultRedisGtidCollectorManager extends AbstractCurrentMetaObserver implements RedisGtidCollectorManager, TopElement {

    @Resource(name = "clientPool")
    private XpipeNettyClientKeyedObjectPool clientPool;

    @Autowired
    protected DcMetaCache dcMetaCache;

    @Autowired
    private MultiDcService multiDcService;

    private ScheduledExecutorService scheduled;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create("DefaultRedisGtidCollectorManager"));
    }

    @Override
    protected void handleClusterModified(ClusterMetaComparator comparator) {

        ClusterMeta clusterMeta = comparator.getCurrent();
        comparator.accept(new ClusterComparatorVisitor(clusterMeta));
        for (ShardMeta shardMeta : comparator.getAdded()) {
            addShard(clusterMeta, shardMeta);
        }

    }

    @Override
    protected void handleClusterDeleted(ClusterMeta clusterMeta) {
        //nothing to do
    }

    @Override
    protected void handleClusterAdd(ClusterMeta clusterMeta) {

        for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
            addShard(clusterMeta, shardMeta);
        }

    }

    @Override
    public Set<ClusterType> getSupportClusterTypes() {
        return Collections.singleton(ClusterType.ONE_WAY);
    }

    private void addShard(ClusterMeta clusterMeta, ShardMeta shardMeta) {

        Long clusterDbId = clusterMeta.getDbId();
        Long shardDbId = shardMeta.getDbId();
        int collectInterval;

        String azGroupType = clusterMeta.getAzGroupType();;
        ClusterType azGroupClusterType = StringUtil.isEmpty(azGroupType) ? null : ClusterType.lookup(azGroupType);
        if (azGroupClusterType == ClusterType.SINGLE_DC
            && dcMetaCache.isCurrentShardParentCluster(clusterDbId, shardDbId)) {
            collectInterval = DefaultRedisGtidCollector.MASTER_DC_SHARD_DIRECTLY_UNDER_CLUSTER_INTERVAL_SECONDS;
        } else {
            collectInterval = DefaultRedisGtidCollector.DEFAULT_INTERVAL_SECONDS;
        }

        RedisGtidCollector redisGtidCollector = new DefaultRedisGtidCollector(clusterDbId, shardDbId, dcMetaCache,
                currentMetaManager, multiDcService, scheduled, clientPool, collectInterval);

        try {
            logger.info("[addShard]cluster_{}, shard_{}", clusterDbId, shardDbId);
            redisGtidCollector.start();
            registerJob(clusterDbId, shardDbId, redisGtidCollector);
        } catch (Exception e) {
            logger.error("[addShard]cluster_{}, shard_{}", clusterDbId, shardDbId, e);
        }
    }

    protected class ClusterComparatorVisitor implements MetaComparatorVisitor<ShardMeta> {

        private ClusterMeta clusterMeta;

        public ClusterComparatorVisitor(ClusterMeta clusterMeta) {
            this.clusterMeta = clusterMeta;
        }

        @Override
        public void visitAdded(ShardMeta added) {
            logger.info("[visitAdded][add shard]{}", added);
            addShard(clusterMeta, added);
        }

        @Override
        public void visitModified(MetaComparator comparator) {

            ShardMetaComparator shardMetaComparator = (ShardMetaComparator) comparator;
            ShardComparatorVisitor shardComparatorVisitor = new ShardComparatorVisitor();
            shardMetaComparator.accept(shardComparatorVisitor);

            Long clusterDbId = clusterMeta.getDbId();
            ShardMeta shardMeta = shardMetaComparator.getFuture();

            if (shardComparatorVisitor.isRedisMetaChanged()) {
                List<RedisMeta> oldRedisMetas = currentMetaManager.getRedises(clusterDbId, shardMeta.getDbId());
                List<RedisMeta> newRedisMetas = shardMeta.getRedises();
                List<RedisMeta> redises = generateNewRedises(oldRedisMetas, newRedisMetas);

                currentMetaManager.setRedises(clusterDbId, shardMeta.getDbId(), redises);
            }
        }

        @Override
        public void visitRemoved(ShardMeta removed) {}

        private List<RedisMeta> generateNewRedises(List<RedisMeta> oldRedisMetas, List<RedisMeta> newRedisMetas) {

            List<RedisMeta> result = new LinkedList<>();

            for (RedisMeta newRedisMeta : newRedisMetas) {
                boolean found = false;
                for (RedisMeta oldRedisMeta : oldRedisMetas) {
                    if (oldRedisMeta.getIp().equals(newRedisMeta.getIp()) &&
                        oldRedisMeta.getPort().equals(newRedisMeta.getPort())) {
                        result.add(oldRedisMeta);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    result.add(MetaCloneFacade.INSTANCE.clone(newRedisMeta));
                }
            }
            return result;
        }
    }

    protected class ShardComparatorVisitor implements MetaComparatorVisitor<InstanceNode> {

        private boolean redisMetaChanged = false;

        @Override
        public void visitAdded(InstanceNode added) {
            if (added instanceof RedisMeta) {
                redisMetaChanged = true;
            }
        }

        @Override
        public void visitModified(MetaComparator comparator) {
            // nothing to do
        }

        @Override
        public void visitRemoved(InstanceNode removed) {
            if (removed instanceof RedisMeta) {
                redisMetaChanged = true;
            }
        }

        public boolean isRedisMetaChanged() {
            return redisMetaChanged;
        }
    }


    @Override
    protected void doDispose() throws Exception {
        scheduled.shutdownNow();
        super.doDispose();
    }
}
