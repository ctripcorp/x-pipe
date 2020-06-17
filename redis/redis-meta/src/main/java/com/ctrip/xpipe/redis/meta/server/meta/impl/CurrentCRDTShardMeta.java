package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CurrentCRDTShardMeta extends AbstractCurrentShardMeta {

    Map<String, RedisMeta> peerMasters = new ConcurrentHashMap<>();

    public CurrentCRDTShardMeta(@JsonProperty("clusterId") String clusterId, @JsonProperty("shardId") String shardId) {
        super(clusterId, shardId);
    }

    public void setPeerMaster(String dcId, RedisMeta peerMaster) {
        if (null == peerMaster) return;
        peerMasters.put(dcId.toLowerCase(), clonePeerMaster(peerMaster));
    }

    public RedisMeta getPeerMaster(String dcId) {
        RedisMeta peerMaster = peerMasters.get(dcId.toLowerCase());
        if (null == peerMaster) return null;
        return clonePeerMaster(peerMaster);
    }

    public void removePeerMaster(String dcId) {
        peerMasters.remove(dcId.toLowerCase());
    }

    public Set<String> getKnownDcs() {
        return new HashSet<>(peerMasters.keySet());
    }

    public List<RedisMeta> getAllPeerMasters() {
        return new ArrayList<>(peerMasters.values());
    }

    private RedisMeta clonePeerMaster(RedisMeta peerMaster) {
        return new RedisMeta().setGid(peerMaster.getGid()).setIp(peerMaster.getIp()).setPort(peerMaster.getPort());
    }

}
