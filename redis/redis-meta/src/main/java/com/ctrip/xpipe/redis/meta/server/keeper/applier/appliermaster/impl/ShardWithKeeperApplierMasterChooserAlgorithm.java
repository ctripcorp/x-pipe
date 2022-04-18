package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/11 17:07
 */
public class ShardWithKeeperApplierMasterChooserAlgorithm extends AbstractApplierMasterChooserAlgorithm {

    public ShardWithKeeperApplierMasterChooserAlgorithm(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                                 CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
        super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
    }

    @Override
    protected Pair<String, Integer> doChoose() {

        KeeperMeta activeKeeper = currentMetaManager.getKeeperActive(clusterDbId, shardDbId);

        if (activeKeeper == null) {
            return null;
        }

        return new Pair<>(activeKeeper.getIp(), activeKeeper.getPort());
    }
}
