package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.InstanceNodeComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Re-runs keeper active election when strategy or keeper priority changes.
 */
@Component
public class KeeperElectReElectService {

    private static final Logger logger = LoggerFactory.getLogger(KeeperElectReElectService.class);

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Autowired
    private KeeperActiveElectAlgorithmManager keeperActiveElectAlgorithmManager;

    @Autowired
    private DcMetaCache dcMetaCache;

    public void reElectAll() {
        for (Long clusterDbId : currentMetaManager.allClusters()) {
            ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterDbId);
            if (clusterMeta == null) {
                continue;
            }
            for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
                try {
                    reElect(clusterDbId, shardMeta.getDbId());
                } catch (Exception e) {
                    logger.error("[reElectAll]cluster_{},shard_{}", clusterDbId, shardMeta.getDbId(), e);
                }
            }
        }
    }

    public void reElect(Long clusterDbId, Long shardDbId) {
        List<KeeperMeta> surviveKeepers = currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId);
        if (surviveKeepers == null || surviveKeepers.isEmpty()) {
            return;
        }

        List<KeeperMeta> enriched = KeeperMetaEnricher.enrich(dcMetaCache, clusterDbId, shardDbId, surviveKeepers);
        KeeperActiveElectAlgorithm algorithm = keeperActiveElectAlgorithmManager.get(clusterDbId, shardDbId);
        KeeperMeta activeKeeper = algorithm.select(clusterDbId, shardDbId, enriched);
        if (activeKeeper == null) {
            logger.warn("[reElect][no active keeper]cluster_{},shard_{},{}", clusterDbId, shardDbId, enriched);
            return;
        }

        logger.info("[reElect]cluster_{},shard_{},active={},survive={}", clusterDbId, shardDbId, activeKeeper, enriched);
        currentMetaManager.setSurviveKeepers(clusterDbId, shardDbId, enriched, activeKeeper);
    }

    public boolean keeperPriorityChanged(ShardMetaComparator shardMetaComparator) {
        for (Object comparator : shardMetaComparator.getMofified()) {
            if (!(comparator instanceof InstanceNodeComparator)) {
                continue;
            }
            InstanceNodeComparator instanceNodeComparator = (InstanceNodeComparator) comparator;
            if (instanceNodeComparator.getCurrent() instanceof KeeperMeta
                    && instanceNodeComparator.getFuture() instanceof KeeperMeta) {
                KeeperMeta current = (KeeperMeta) instanceNodeComparator.getCurrent();
                KeeperMeta future = (KeeperMeta) instanceNodeComparator.getFuture();
                if (!java.util.Objects.equals(current.getPriority(), future.getPriority())) {
                    return true;
                }
            }
        }
        return false;
    }
}
