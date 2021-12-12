package com.ctrip.xpipe.redis.meta.server.crdt.master;

public interface MasterChooseCommandFactory {

    MasterChooseCommand buildPeerMasterChooserCommand(String dcId, Long clusterDbId, Long shardDbId);

    MasterChooseCommand buildCurrentMasterChooserCommand(Long clusterDbId, Long shardDbId);

}
