package com.ctrip.xpipe.redis.meta.server.crdt.event;

import com.ctrip.xpipe.observer.AbstractEvent;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;

public class RemotePeerMasterChangeEvent extends AbstractEvent<ClusterMeta> {

    private String dcId;

    private String clusterId;

    private String shardId;

    public RemotePeerMasterChangeEvent(String dcId, String clusterId, String shardId) {
        this.dcId = dcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
    }

    public String getDcId() {
        return dcId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getShardId() {
        return shardId;
    }

    @Override
    public String toString() {
        return "RemotePeerMasterChange:";
    }

}
