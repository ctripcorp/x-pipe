package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommandFactory;
import com.ctrip.xpipe.redis.meta.server.crdt.master.PeerMasterChooseAction;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.PEER_MASTER_CHOOSE_EXECUTOR;

@Component
public class DefaultPeerMasterChooseAction implements PeerMasterChooseAction {

    private MasterChooseCommandFactory masterChooseCommandFactory;

    private KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterChooseExecutors;

    @Autowired
    public DefaultPeerMasterChooseAction(MasterChooseCommandFactory masterChooseCommandFactory,
                                         @Qualifier(PEER_MASTER_CHOOSE_EXECUTOR) KeyedOneThreadTaskExecutor<Pair<String, String> > peerMasterChooseExecutors) {
        this.masterChooseCommandFactory = masterChooseCommandFactory;
        this.peerMasterChooseExecutors = peerMasterChooseExecutors;
    }

    @Override
    public void choosePeerMaster(String dcId, String clusterId, String shardId) {
        MasterChooseCommand chooseCommand = masterChooseCommandFactory.buildPeerMasterChooserCommand(dcId, clusterId, shardId);
        if (null != chooseCommand) this.peerMasterChooseExecutors.execute(Pair.of(clusterId, shardId), chooseCommand);
    }

}
