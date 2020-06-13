package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CurrentCRDTShardMeta extends AbstractCurrentShardMeta {

    Map<String, RedisMeta> peerMasters = new ConcurrentHashMap<>();

    public CurrentCRDTShardMeta(@JsonProperty("clusterId") String clusterId, @JsonProperty("shardId") String shardId) {
        super(clusterId, shardId);
    }

    public void setPeerMaster(String dcId, RedisMeta peerMaster) {
        peerMasters.put(dcId, peerMaster);
    }

    public RedisMeta getPeerMaster(String dcId) {
        return peerMasters.get(dcId);
    }

    public void removePeerMaster(String dcId) {
        peerMasters.remove(dcId);
    }

    public Set<String> getRelatedDcs() {
        return peerMasters.keySet();
    }

}
