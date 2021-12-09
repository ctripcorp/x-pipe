package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.crdt.AbstractCurrentPeerMasterMetaObserver;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommandFactory;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooserManager;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.MasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.PEER_MASTER_CHOOSE_EXECUTOR;

@Component
public class DefaultMasterChooserManager extends AbstractCurrentPeerMasterMetaObserver implements MasterChooserManager {

    @Autowired
    protected DcMetaCache dcMetaCache;

    @Autowired
    private MasterChooseCommandFactory chooseCommandFactory;

    @Resource(name = PEER_MASTER_CHOOSE_EXECUTOR)
    private KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterChooseExecutor;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    protected ScheduledExecutorService scheduled;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("PeerMasterChooserSchedule"));
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        scheduled.shutdownNow();
    }

    @Override
    protected void addShard(String clusterId, String shardId) {
        try {
            logger.info("[addShard]{}, {}", clusterId, shardId);

            CurrentMasterChooser currentMasterChooser = new CurrentMasterChooser(clusterId, shardId, dcMetaCache, currentMetaManager,
                    chooseCommandFactory, executors, peerMasterChooseExecutor, scheduled);
            currentMasterChooser.start();

            MasterChooser peerMasterChooser = new PeerMasterChooser(clusterId, shardId, dcMetaCache, currentMetaManager,
                    chooseCommandFactory, executors, peerMasterChooseExecutor, scheduled);
            peerMasterChooser.start();

            registerJob(clusterId, shardId, currentMasterChooser);
            registerJob(clusterId, shardId, peerMasterChooser);

        } catch (Exception e) {
            logger.error("[addShard]{}, {}", clusterId, shardId, e);
        }
    }

}
