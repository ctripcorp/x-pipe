package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl;

import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.MasterChooser;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.AbstractClusterShardPeriodicTask;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/11 15:07
 */
public abstract class AbstractApplierMasterChooser extends AbstractClusterShardPeriodicTask implements MasterChooser {

    public static int DEFAULT_APPLIER_MASTER_CHECK_INTERVAL_SECONDS = Integer
            .parseInt(System.getProperty("APPLIER_MASTER_CHECK_INTERVAL_SECONDS", "5"));

    protected int checkIntervalSeconds;

    public AbstractApplierMasterChooser(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                       CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
        this(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled,
                DEFAULT_APPLIER_MASTER_CHECK_INTERVAL_SECONDS);
    }

    public AbstractApplierMasterChooser(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                       CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, int checkIntervalSeconds) {
        super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
        this.checkIntervalSeconds = checkIntervalSeconds;
    }

    @Override
    protected void work() {
        Pair<String, Integer> applierMaster = chooseApplierMaster();
        logger.debug("[doRun]cluster_{}, shard_{}, {}", clusterDbId, shardDbId, applierMaster);
        Pair<String, Integer> currentMaster = currentMetaManager.getApplierMaster(clusterDbId, shardDbId);
        if (applierMaster == null || applierMaster.equals(currentMaster)) {
            logger.debug("[doRun][new master null or equals old master]{}", applierMaster);
            return;
        }

        String srcSids = currentMetaManager.getSrcSids(clusterDbId, shardDbId);
        logger.debug("[doRun][set]cluster_{}, shard_{}, {}, sid_{}", clusterDbId, shardDbId, applierMaster, srcSids);

        currentMetaManager.setApplierMasterAndNotify(clusterDbId, shardDbId, applierMaster.getKey(), applierMaster.getValue(), srcSids);
    }

    @Override
    protected int getWorkIntervalSeconds() {
        return checkIntervalSeconds;
    }

    protected abstract Pair<String, Integer> chooseApplierMaster();
}
