package com.ctrip.xpipe.redis.meta.server.crdt.master;

public interface MasterChooseCommandFactory {

    MasterChooseCommand buildPeerMasterChooserCommand(String dcId, String clusterId, String shardId);

    MasterChooseCommand buildCurrentMasterChooserCommand(String clusterId, String shardId);

}
