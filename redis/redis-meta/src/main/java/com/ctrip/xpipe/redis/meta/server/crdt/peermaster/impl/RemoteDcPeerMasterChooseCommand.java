package com.ctrip.xpipe.redis.meta.server.crdt.peermaster.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;


public class RemoteDcPeerMasterChooseCommand extends AbstractPeerMasterChooseCommand {

    private MultiDcService multiDcService;

    public RemoteDcPeerMasterChooseCommand(String dcId, String clusterId, String shardId, MultiDcService multiDcService) {
        super(dcId, clusterId, shardId);
        this.multiDcService = multiDcService;
    }

    @Override
    public RedisMeta choose() {
        return multiDcService.getPeerMaster(dcId, clusterId, shardId);
    }

}
