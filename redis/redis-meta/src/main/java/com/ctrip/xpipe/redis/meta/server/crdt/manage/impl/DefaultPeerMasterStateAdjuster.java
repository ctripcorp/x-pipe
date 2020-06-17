package com.ctrip.xpipe.redis.meta.server.crdt.manage.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.OneThreadTaskExecutor;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.crdt.manage.PeerMasterStateAdjuster;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DefaultPeerMasterStateAdjuster extends AbstractStartStoppable implements PeerMasterStateAdjuster {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final int DEFAULT_PEER_MASTER_ADJUST_INTERVAL_SECONDS = Integer
            .parseInt(System.getProperty("PEER_MASTER_ADJUST_INTERVAL_SECONDS", "60"));

    protected DcMetaCache dcMetaCache;

    protected CurrentMetaManager currentMetaManager;

    private Executor executors;

    private OneThreadTaskExecutor adjustTaskExecutor;

    protected ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    protected String clusterId, shardId;

    protected int adjustIntervalSeconds;

    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    public DefaultPeerMasterStateAdjuster(String clusterId, String shardId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                          XpipeNettyClientKeyedObjectPool keyedObjectPool, Executor executors, ScheduledExecutorService scheduled) {
        this(clusterId, shardId, dcMetaCache, currentMetaManager, keyedObjectPool, executors, scheduled, DEFAULT_PEER_MASTER_ADJUST_INTERVAL_SECONDS);
    }

    public DefaultPeerMasterStateAdjuster(String clusterId, String shardId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                          XpipeNettyClientKeyedObjectPool keyedObjectPool, Executor executors,
                                          ScheduledExecutorService scheduled, int adjustIntervalSeconds) {
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.dcMetaCache = dcMetaCache;
        this.currentMetaManager = currentMetaManager;
        this.keyedObjectPool = keyedObjectPool;
        this.executors = executors;
        this.adjustTaskExecutor = new OneThreadTaskExecutor(executors);
        this.scheduled = scheduled;
        this.adjustIntervalSeconds = adjustIntervalSeconds;
    }

    @Override
    protected void doStart() throws Exception {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                adjust();
            }
        }, 0, adjustIntervalSeconds, TimeUnit.SECONDS);
    }

    public void adjust() {
        logger.debug("[DefaultPeerMAsterStateAdjuster] begin for {} {}", clusterId, shardId);
        Set<String> relatedDcs = dcMetaCache.getRelatedDcs(clusterId, shardId);
        Set<String> knownDcs = currentMetaManager.getPeerMasterKnownDcs(clusterId, shardId);

        if (knownDcs.isEmpty()) {
            logger.info("[adjust][{}][{}] unknown any dcs, skip adjust", clusterId, shardId);
            return;
        } else if (knownDcs.size() > relatedDcs.size()) {
            knownDcs.stream().filter(knownDc -> !relatedDcs.contains(knownDc)).collect(Collectors.toSet()).forEach(dcId -> {
                currentMetaManager.removePeerMaster(dcId, clusterId, shardId);
                knownDcs.remove(dcId);
            });
        }

        doPeerMasterAdjust();
    }

    public void clearPeerMaster() {
        RedisMeta currentMaster = currentMetaManager.getPeerMaster(dcMetaCache.getCurrentDc(), clusterId, shardId);
        if (null == currentMaster) {
            logger.info("[clearPeerMaster][{}][{}] unknown master, skip clear", clusterId, shardId);
            return;
        }

        this.adjustTaskExecutor.executeCommand(new PeerMasterAdjustJob(clusterId, shardId, Collections.emptyList(),
                Pair.of(currentMaster.getIp(), currentMaster.getPort()), true,
                keyedObjectPool.getKeyPool(new DefaultEndPoint(currentMaster.getIp(), currentMaster.getPort())),
                scheduled, executors));
    }

    private void doPeerMasterAdjust() {
        RedisMeta currentMaster = currentMetaManager.getPeerMaster(dcMetaCache.getCurrentDc(), clusterId, shardId);
        if (null == currentMaster) {
            logger.info("[doPeerMasterAdjust][{}][{}] unknown master, skip adjust", clusterId, shardId);
            return;
        }

        List<RedisMeta> allPeerMasters = currentMetaManager.getAllPeerMasters(clusterId, shardId);
        allPeerMasters.remove(currentMaster);
        this.adjustTaskExecutor.executeCommand(new PeerMasterAdjustJob(clusterId, shardId, allPeerMasters,
                Pair.of(currentMaster.getIp(), currentMaster.getPort()), false,
                keyedObjectPool.getKeyPool(new DefaultEndPoint(currentMaster.getIp(), currentMaster.getPort())),
                scheduled, executors));
    }

    @Override
    protected void doStop() throws Exception {
        if (future != null) {
            logger.info("[doStop]");
            future.cancel(true);
        }
    }

    @Override
    public void release() throws Exception {
        stop();
    }

    @VisibleForTesting
    protected void setAdjustTaskExecutor(OneThreadTaskExecutor oneThreadTaskExecutor) {
        this.adjustTaskExecutor = oneThreadTaskExecutor;
    }

}
