package com.ctrip.xpipe.redis.meta.server.crdt.replication;

import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;

public interface PeerMasterAdjustJobFactory {

    PeerMasterAdjustJob buildPeerMasterAdjustJob(String clusterId, String shardId);

}
