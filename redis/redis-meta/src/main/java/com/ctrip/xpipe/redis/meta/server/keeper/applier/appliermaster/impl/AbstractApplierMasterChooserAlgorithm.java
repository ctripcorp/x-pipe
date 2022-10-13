package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl;

import com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.ApplierMasterChooserAlgorithm;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/11 16:59
 */
public abstract class AbstractApplierMasterChooserAlgorithm implements ApplierMasterChooserAlgorithm {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DcMetaCache dcMetaCache;

    protected CurrentMetaManager currentMetaManager;

    protected Long clusterDbId, shardDbId;

    protected ScheduledExecutorService scheduled;


    public AbstractApplierMasterChooserAlgorithm(long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                                CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {

        this.dcMetaCache = dcMetaCache;
        this.currentMetaManager = currentMetaManager;
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
        this.scheduled = scheduled;
    }

    @Override
    public Pair<String, Integer> choose() {
        return doChoose();
    }

    protected abstract Pair<String, Integer> doChoose();
}
