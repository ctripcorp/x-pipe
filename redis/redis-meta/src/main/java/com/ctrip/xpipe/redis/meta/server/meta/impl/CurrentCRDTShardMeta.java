package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CurrentCRDTShardMeta extends AbstractCurrentShardMeta {

    RedisMeta currentMaster;

    Map<String, RedisMeta> peerMasters = new ConcurrentHashMap<>();

    public CurrentCRDTShardMeta(@JsonProperty("clusterDbId") Long clusterDbId, @JsonProperty("shardDbId") Long shardDbId) {
        super(clusterDbId, shardDbId);
    }

    public void setCurrentMaster(RedisMeta master) {
        if (null == master) return;
        this.currentMaster = cloneMasterMeta(master);
    }

    public RedisMeta getCurrentMaster() {
        return cloneMasterMeta(currentMaster);
    }

    public void setPeerMaster(String dcId, RedisMeta peerMaster) {
        if (null == peerMaster) return;
        peerMasters.put(dcId.toLowerCase(), cloneMasterMeta(peerMaster));
    }

    public RedisMeta getPeerMaster(String dcId) {
        RedisMeta peerMaster = peerMasters.get(dcId.toLowerCase());
        return cloneMasterMeta(peerMaster);
    }

    public void removePeerMaster(String dcId) {
        peerMasters.remove(dcId.toLowerCase());
    }

    public Set<String> getUpstreamPeerDcs() {
        return new HashSet<>(peerMasters.keySet());
    }

    public Map<String, RedisMeta> getAllPeerMasters() {
        return new ConcurrentHashMap<>(peerMasters);
    }

    private RedisMeta cloneMasterMeta(RedisMeta peerMaster) {
        if (null == peerMaster) return null;
        return new RedisMeta().setGid(peerMaster.getGid()).setIp(peerMaster.getIp()).setPort(peerMaster.getPort());
    }

}
