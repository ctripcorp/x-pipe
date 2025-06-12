package com.ctrip.xpipe.redis.meta.server.keeper.applier;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.job.ApplierStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author ayq
 * <p>
 * 2022/4/11 15:20
 */
@Component
public class DefaultApplierStateChangeHandler extends AbstractLifecycle implements MetaServerStateChangeHandler, TopElement {

    protected static Logger logger = LoggerFactory.getLogger(DefaultApplierStateChangeHandler.class);

    @Resource(name = MetaServerContextConfig.CLIENT_POOL)
    private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private ExecutorService executors;

    private KeyedOneThreadTaskExecutor<Pair<Long, Long>> keyedOneThreadTaskExecutor;

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Autowired
    private DcMetaCache dcMetaCache;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        executors = DefaultExecutorFactory.createAllowCoreTimeout("ApplierStateChangeHandler", OsUtils.defaultMaxCoreThreadCount()).createExecutorService();
        keyedOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<>(executors);
    }

    @Override
    protected void doDispose() throws Exception {

        keyedOneThreadTaskExecutor.destroy();
        executors.shutdown();
        super.doDispose();
    }

    @Override
    public void applierMasterChanged(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster, String srcSids) {

        logger.info("[applierMasterChanged]cluster_{},shard_{},{}", clusterDbId, shardDbId, newMaster);
        ApplierMeta activeApplier = currentMetaManager.getApplierActive(clusterDbId, shardDbId);

        if (activeApplier == null) {
            logger.info("[applierMasterChanged][no active applier, do nothing]cluster_{},shard_{},{}", clusterDbId, shardDbId, newMaster);
            return;
        }
        if (!activeApplier.isActive()) {
            throw new IllegalStateException("[active applier not active]{}" + activeApplier);
        }

        logger.info("[applierMasterChanged][set active applier master]{}, {}", activeApplier, newMaster);

        List<ApplierMeta> appliers = new LinkedList<>();
        appliers.add(activeApplier);
        GtidSet gtidSet = currentMetaManager.getGtidSet(clusterDbId, srcSids);

        keyedOneThreadTaskExecutor.execute(
                new Pair<>(clusterDbId, shardDbId),
                createApplierStateChangeJob(clusterDbId, shardDbId, appliers, newMaster, srcSids, gtidSet));
    }

    @Override
    public void applierActiveElected(Long clusterDbId, Long shardDbId, ApplierMeta activeApplier, String srcSids) {

        logger.info("[applierActiveElected]cluster_{},shard_{},{}", clusterDbId, shardDbId, activeApplier);

        List<ApplierMeta> appliers = currentMetaManager.getSurviveAppliers(clusterDbId, shardDbId);
        if (appliers == null || appliers.isEmpty()) {
            logger.info("[applierActiveElected][none applier survive, do nothing]");
            return;
        }
        Pair<String, Integer> activeApplierMaster = currentMetaManager.getApplierMaster(clusterDbId, shardDbId);

        GtidSet gtidSet = currentMetaManager.getGtidSet(clusterDbId, srcSids);
        ApplierStateChangeJob applierStateChangeJob = createApplierStateChangeJob(clusterDbId, shardDbId, appliers,
                activeApplierMaster, srcSids, gtidSet);

        keyedOneThreadTaskExecutor.execute(new Pair<>(clusterDbId, shardDbId), applierStateChangeJob);
    }

    private ApplierStateChangeJob createApplierStateChangeJob(Long clusterDbId, Long shardDbId, List<ApplierMeta> appliers,
              Pair<String, Integer> master, String sids, GtidSet gtidSet) {

        String dstDcId;
        if (dcMetaCache.getShardKeepers(clusterDbId, shardDbId).isEmpty()) {
            dstDcId = dcMetaCache.getUpstreamDc(dcMetaCache.getCurrentDc(), clusterDbId, shardDbId);
        } else {
            dstDcId = dcMetaCache.getCurrentDc();
        }
        RouteMeta routeMeta = currentMetaManager.getClusterRouteByDcId(dstDcId, clusterDbId);
        return new ApplierStateChangeJob(appliers, master, sids, gtidSet, routeMeta, clientPool, scheduled, executors);
    }

    @VisibleForTesting
    public void setCurrentMetaManager(CurrentMetaManager currentMetaManager) {
        this.currentMetaManager = currentMetaManager;
    }

    @VisibleForTesting
    public void setDcMetaCache(DcMetaCache dcMetaCache) {
        this.dcMetaCache = dcMetaCache;
    }

    @VisibleForTesting
    public void setClientPool(SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool) {
        this.clientPool = clientPool;
    }

    @VisibleForTesting
    public void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @VisibleForTesting
    public void setExecutors(ExecutorService executors) {
        this.executors = executors;
    }
}
