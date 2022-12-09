package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/11 16:56
 */
public class ShardWithoutKeeperApplierMasterChooserAlgorithm extends AbstractApplierMasterChooserAlgorithm {

    private MultiDcService multiDcService;

    public ShardWithoutKeeperApplierMasterChooserAlgorithm(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                                           CurrentMetaManager currentMetaManager, MultiDcService multiDcService, ScheduledExecutorService scheduled) {
        super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
        this.multiDcService = multiDcService;
    }

    @Override
    protected Pair<String, Integer> doChoose() {

        String dcName = dcMetaCache.getUpstreamDc(dcMetaCache.getCurrentDc(), clusterDbId, shardDbId);

        KeeperMeta keeperMeta = multiDcService.getActiveKeeper(dcName, clusterDbId, shardDbId);
        logger.debug("[doChooseApplierMaster]{}, cluster_{}, shard_{}, {}", dcName, clusterDbId, shardDbId, keeperMeta);
        if(keeperMeta == null){
            return null;
        }
        return new Pair<>(keeperMeta.getIp(), keeperMeta.getPort());
    }
}
