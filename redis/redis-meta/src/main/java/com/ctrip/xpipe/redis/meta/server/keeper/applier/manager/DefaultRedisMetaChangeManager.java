package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.RedisMetaChangeManager;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.Set;

/**
 * @author ayq
 * <p>
 * 2022/4/25 23:23
 */
@Component
public class DefaultRedisMetaChangeManager extends AbstractCurrentMetaObserver implements RedisMetaChangeManager, TopElement {

    @Autowired
    private DcMetaCache dcMetaCache;

    @Autowired
    private MultiDcService multiDcService;

    @Override
    protected void handleClusterAdd(ClusterMeta clusterMeta) {
        //nothing to do
    }

    @Override
    protected void handleClusterModified(ClusterMetaComparator comparator) {

        Long clusterDbId = comparator.getCurrent().getDbId();

        Set<MetaComparator> shardMetaComparators = comparator.getMofified();
        for (MetaComparator shardMetaComparator : shardMetaComparators) {
            ShardMeta shardMeta = ((ShardMetaComparator) shardMetaComparator).getFuture();
            Long shardDbId = shardMeta.getDbId();

            String sids = currentMetaManager.getSids(clusterDbId, shardDbId);
            if (StringUtils.isEmpty(sids)) {
                logger.warn("[handleClusterModified] sid empty, cluster_{}, shard_{}", clusterDbId, shardDbId);
                return;
            }

            Set<String> downstreamDcs = dcMetaCache.getDownstreamDcs(dcMetaCache.getCurrentDc(), clusterDbId, shardDbId);
            for (String downstreamDc : downstreamDcs) {
                multiDcService.sidsChange(downstreamDc, clusterDbId, shardDbId, sids);
            }
        }
    }

    @Override
    protected void handleClusterDeleted(ClusterMeta clusterMeta) {
        //nothing to do
    }

    @Override
    public Set<ClusterType> getSupportClusterTypes() {
        return Collections.singleton(ClusterType.HETERO);
    }

    @VisibleForTesting
    public void setMultiDcService(MultiDcService multiDcService) {
        this.multiDcService = multiDcService;
    }

    @VisibleForTesting
    public void setDcMetaCache(DcMetaCache dcMetaCache) {
        this.dcMetaCache = dcMetaCache;
    }
}
