package com.ctrip.xpipe.redis.meta.server.crdt.manage;

public interface PeerMasterAdjusterManager {

    PeerMasterStateAdjuster getAdjuster(String clusterId, String shardId);

}
