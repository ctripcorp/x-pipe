package com.ctrip.xpipe.redis.meta.server.crdt.master;

public interface PeerMasterChooseAction {

    void choosePeerMaster(String dcId, Long clusterDbId, Long shardDbId);

}
