package com.ctrip.xpipe.redis.meta.server.crdt.peermaster.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class DefaultPeerMasterChooser extends AbstractStartStoppable implements PeerMasterChooser {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final int DEFAULT_PEER_MASTER_CHECK_INTERVAL_SECONDS = Integer
            .parseInt(System.getProperty("PEER_MASTER_CHECK_INTERVAL_SECONDS", "10"));

    protected DcMetaCache dcMetaCache;

    protected CurrentMetaManager currentMetaManager;

    private Executor executors;

    protected ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    protected String clusterId, shardId;

    protected int checkIntervalSeconds;

    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    private MultiDcService multiDcService;

    public DefaultPeerMasterChooser(String clusterId, String shardId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                    XpipeNettyClientKeyedObjectPool keyedObjectPool, MultiDcService multiDcService,
                                    Executor executors, ScheduledExecutorService scheduled) {
        this(clusterId, shardId, dcMetaCache, currentMetaManager, keyedObjectPool, multiDcService, executors, scheduled,
                DEFAULT_PEER_MASTER_CHECK_INTERVAL_SECONDS);
    }

    public DefaultPeerMasterChooser(String clusterId, String shardId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                    XpipeNettyClientKeyedObjectPool keyedObjectPool, MultiDcService multiDcService,
                                    Executor executors, ScheduledExecutorService scheduled, int checkIntervalSeconds) {
        this.dcMetaCache = dcMetaCache;
        this.currentMetaManager = currentMetaManager;
        this.executors = executors;
        this.scheduled = scheduled;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.checkIntervalSeconds = checkIntervalSeconds;
        this.keyedObjectPool = keyedObjectPool;
        this.multiDcService = multiDcService;
    }

    @Override
    protected void doStart() throws Exception {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterId);

                ParallelCommandChain parallelCommandChain = new ParallelCommandChain(executors);
                for (String dcName : clusterMeta.getDcs().split("\\s*,\\s*")) {
                    parallelCommandChain.add(createMasterChooserCommand(dcName));
                }
                parallelCommandChain.execute();
            }
        }, 0, checkIntervalSeconds, TimeUnit.SECONDS);
    }

    public PeerMasterChooseCommand createMasterChooserCommand(String dcName) {
        PeerMasterChooseCommand peerMasterChooseCommand = null;
        if (dcMetaCache.getCurrentDc().equalsIgnoreCase(dcName)) {
            peerMasterChooseCommand = new DefaultPeerMasterChooseCommand(dcName, clusterId, shardId, dcMetaCache,
                    scheduled, keyedObjectPool, checkIntervalSeconds / 2 );
        } else {
            peerMasterChooseCommand = new RemoteDcPeerMasterChooseCommand(dcName, clusterId, shardId, multiDcService);
        }

        return wrapMasterChooseCommand(dcName, peerMasterChooseCommand);
    }

    @VisibleForTesting
    protected PeerMasterChooseCommand wrapMasterChooseCommand(String dcName, PeerMasterChooseCommand command) {
        command.future().addListener(new CommandFutureListener<RedisMeta>() {
            @Override
            public void operationComplete(CommandFuture<RedisMeta> commandFuture) throws Exception {
                logger.debug("[doRun]{}, {}, {}", dcName, clusterId, shardId);
                if (commandFuture.isSuccess()) {
                    RedisMeta peerMaster = commandFuture.get();
                    RedisMeta currentPeerMaster = currentMetaManager.getPeerMaster(dcName, clusterId, shardId);

                    if (null == peerMaster || (null != currentPeerMaster
                            && peerMaster.getGid().equals(currentPeerMaster.getGid())
                            && peerMaster.getIp().equals(currentPeerMaster.getIp())
                            && peerMaster.getPort().equals(currentPeerMaster.getPort()))) {
                        logger.debug("[doRun][new peer master master null or equals old master]{}", peerMaster);
                        return;
                    }

                    logger.debug("[doRun][set]{}, {}, {}, {}", dcName, clusterId, shardId, peerMaster);
                    currentMetaManager.setPeerMaster(dcName, clusterId, shardId, peerMaster.getGid(), peerMaster.getIp(), peerMaster.getPort());
                }
            }
        });

        return command;
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

}
