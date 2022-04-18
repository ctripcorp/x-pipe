package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/8 12:09
 */
public class HeteroDownStreamDcKeeperMasterChooserAlgorithm extends AbstractKeeperMasterChooserAlgorithm {

    private MultiDcService multiDcService;

    public HeteroDownStreamDcKeeperMasterChooserAlgorithm(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                                CurrentMetaManager currentMetaManager, MultiDcService multiDcService, ScheduledExecutorService scheduled) {
        super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
        this.multiDcService = multiDcService;
    }

    @Override
    protected Pair<String, Integer> doChoose() {

        String upstreamDc = dcMetaCache.getUpstreamDc(dcMetaCache.getCurrentDc(), clusterDbId, shardDbId);

        KeeperMeta keeperMeta = multiDcService.getActiveKeeper(upstreamDc, clusterDbId, shardDbId);
        logger.debug("[doChooseKeeperMaster]{}, cluster_{}, shard_{}, {}", upstreamDc, clusterDbId, shardDbId, keeperMeta);
        if(keeperMeta == null){
            return null;
        }
        return new Pair<>(keeperMeta.getIp(), keeperMeta.getPort());
    }
}
