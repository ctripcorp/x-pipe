package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.RedisGtidCollector;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.RedisGtidCollectorManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        Long clusterDbId = comparator.getCurrent().getDbId();
        for (ShardMeta shardMeta : comparator.getAdded()) {
            addShard(clusterDbId, shardMeta);
        }

    }

    @Override
    protected void handleClusterDeleted(ClusterMeta clusterMeta) {
        //nothing to do
    }

    @Override
    protected void handleClusterAdd(ClusterMeta clusterMeta) {

        for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
            addShard(clusterMeta.getDbId(), shardMeta);
        }

    }

    @Override
    public Set<ClusterType> getSupportClusterTypes() {
        return Stream.of(ClusterType.HETERO).collect(Collectors.toSet());
    }

    private void addShard(Long clusterDbId, ShardMeta shardMeta) {

        Long shardDbId = shardMeta.getDbId();

        RedisGtidCollector redisGtidCollector = new DefaultRedisGtidCollector(clusterDbId, shardDbId, dcMetaCache,
                currentMetaManager, multiDcService, scheduled, clientPool);

        try {
            logger.info("[addShard]cluster_{}, shard_{}", clusterDbId, shardDbId);
            redisGtidCollector.start();
            registerJob(clusterDbId, shardDbId, redisGtidCollector);
        } catch (Exception e) {
            logger.error("[addShard]cluster_{}, shard_{}", clusterDbId, shardDbId, e);
        }
    }

    @Override
    protected void doDispose() throws Exception {
        scheduled.shutdownNow();
        super.doDispose();
    }
}
