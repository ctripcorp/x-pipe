package com.ctrip.xpipe.redis.meta.server.crdt;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.crdt.manage.PeerMasterAdjusterManager;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooserManager;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.Executor;

@Component
public class PeerMasterMetaServerStateChangeHandler implements MetaServerStateChangeHandler {

    @Autowired
    private PeerMasterChooserManager peerMasterChooserManager;

    @Autowired
    private PeerMasterAdjusterManager peerMasterAdjusterManager;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    @Override
    public void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) {
        //nothing to do
    }

    @Override
    public void keeperMasterChanged(String clusterId, String shardId, Pair<String, Integer> newMaster) {
        //nothing to do
    }

    @Override
    public void upstreamPeerMasterChange(String dcId, String clusterId, String shardId) {
        peerMasterChooserManager.getChooser(clusterId, shardId).createMasterChooserCommand(dcId).execute(executors);
    }

    @Override
    public void peerMasterChanged(String dcId, String clusterId, String shardId) {
        peerMasterAdjusterManager.getAdjuster(clusterId, shardId).adjust();
    }

}
