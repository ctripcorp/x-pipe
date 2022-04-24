package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl;

import com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.ApplierMasterChooserAlgorithm;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.util.CollectionUtils;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/11 15:07
 */
public class DefaultDcApplierMasterChooser extends AbstractApplierMasterChooser {

    private MultiDcService multiDcService;

    private ApplierMasterChooserAlgorithm applierMasterChooserAlgorithm;

    public DefaultDcApplierMasterChooser(Long clusterDbId, Long shardDbId, MultiDcService multiDcService,
                                        DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
        this(clusterDbId, shardDbId, multiDcService, dcMetaCache, currentMetaManager, scheduled, DEFAULT_APPLIER_MASTER_CHECK_INTERVAL_SECONDS);
    }

    public DefaultDcApplierMasterChooser(Long clusterDbId, Long shardDbId, MultiDcService multiDcService,
                                        DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, int checkIntervalSeconds) {
        super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled, checkIntervalSeconds);
        this.multiDcService = multiDcService;
    }

    @Override
    protected Pair<String, Integer> chooseApplierMaster() {

        if (CollectionUtils.isEmpty(dcMetaCache.getShardAppliers(clusterDbId, shardDbId))) {
            return null;
        }

        if (CollectionUtils.isEmpty(dcMetaCache.getShardKeepers(clusterDbId, shardDbId))) {

            if (applierMasterChooserAlgorithm == null || !(applierMasterChooserAlgorithm instanceof ShardWithoutKeeperApplierMasterChooserAlgorithm)) {

                logger.info("[chooseApplierMaster][current dc with applier, change algorithm]cluster_{}, shard_{}", clusterDbId, shardDbId);
                applierMasterChooserAlgorithm = new ShardWithoutKeeperApplierMasterChooserAlgorithm(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, multiDcService, scheduled);
            }
        } else {
            if (applierMasterChooserAlgorithm == null || !(applierMasterChooserAlgorithm instanceof ShardWithKeeperApplierMasterChooserAlgorithm)) {

                logger.info("[chooseApplierMaster][current dc without applier, change algorithm]cluster_{}, shard_{}", clusterDbId, shardDbId);
                applierMasterChooserAlgorithm = new ShardWithKeeperApplierMasterChooserAlgorithm(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
            }
        }

        return applierMasterChooserAlgorithm.choose();
    }

    @Override
    protected String getSrcSids() {

        String srcDc = dcMetaCache.getSrcDc(dcMetaCache.getCurrentDc(), clusterDbId, shardDbId);
        String upstreamDc = dcMetaCache.getUpstreamDc(dcMetaCache.getCurrentDc(), clusterDbId, shardDbId);

        String sids = multiDcService.getSids(upstreamDc, srcDc, clusterDbId, shardDbId);
        logger.debug("[getSrcSids] upstreamDc_{}, srcDc_{}, cluster_{}, shard_{}, sids_{}", upstreamDc, srcDc, clusterDbId, shardDbId, sids);

        return sids;
    }

    protected ApplierMasterChooserAlgorithm getApplierMasterChooserAlgorithm() {
        return applierMasterChooserAlgorithm;
    }
}
