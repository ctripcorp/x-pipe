package com.ctrip.xpipe.redis.meta.server.crdt.peermaster;

public interface PeerMasterChooserManager {

    PeerMasterChooser getChooser(String clusterId, String shardId);

}
