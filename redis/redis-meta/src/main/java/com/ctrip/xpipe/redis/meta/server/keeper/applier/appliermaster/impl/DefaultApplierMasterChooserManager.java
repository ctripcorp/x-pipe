package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierMasterElector;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.MasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/11 15:00
 */
@Component
public class DefaultApplierMasterChooserManager extends AbstractCurrentMetaObserver implements ApplierMasterElector, TopElement {

    @Autowired
    protected DcMetaCache dcMetaCache;

    @Autowired
    private MultiDcService multiDcService;

    protected ScheduledExecutorService scheduled;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create("DefaultApplierMasterChooserManager"));
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
        return Collections.singleton(ClusterType.HETERO);
    }

    @Override
    protected void doDispose() throws Exception {
        scheduled.shutdownNow();
        super.doDispose();
    }

    private void addShard(Long clusterDbId, ShardMeta shardMeta) {

        Long shardDbId = shardMeta.getDbId();

        MasterChooser applierMasterChooser = new DefaultDcApplierMasterChooser(clusterDbId, shardDbId, multiDcService,
                dcMetaCache, currentMetaManager, scheduled);


        try {
            logger.info("[addShard]cluster_{}, shard_{}, {}", clusterDbId, shardDbId, applierMasterChooser);
            applierMasterChooser.start();
            //release resources
            registerJob(clusterDbId, shardDbId, applierMasterChooser);
        } catch (Exception e) {
            logger.error("[addShard]cluster_{}, shard_{}", clusterDbId, shardDbId);
        }
    }
}
