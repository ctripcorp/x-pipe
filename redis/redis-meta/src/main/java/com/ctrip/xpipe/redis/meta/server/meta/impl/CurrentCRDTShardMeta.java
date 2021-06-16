package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CurrentCRDTShardMeta extends AbstractCurrentShardMeta {

    RedisMeta currentMaster;

    Map<String, ProxyRedisMeta> peerMasters = new ConcurrentHashMap<>();

    public CurrentCRDTShardMeta(@JsonProperty("clusterId") String clusterId, @JsonProperty("shardId") String shardId) {
        super(clusterId, shardId);
    }

    public void setCurrentMaster(RedisMeta master) {
        if (null == master) return;
        this.currentMaster = ProxyRedisMeta.valueof(master);
    }

    public RedisMeta getCurrentMaster() {
        return cloneMasterMeta((ProxyRedisMeta) currentMaster);
    }

    public void setPeerMaster(String dcId, ProxyRedisMeta peerMaster) {
        if (null == peerMaster) return;

        peerMasters.put(dcId.toLowerCase(), cloneMasterMeta(peerMaster));
    }

    public ProxyRedisMeta getPeerMaster(String dcId) {
        ProxyRedisMeta peerMaster = peerMasters.get(dcId.toLowerCase());
        return cloneMasterMeta(peerMaster);
    }

    public void removePeerMaster(String dcId) {
        peerMasters.remove(dcId.toLowerCase());
    }

    public Set<String> getUpstreamPeerDcs() {
        return new HashSet<>(peerMasters.keySet());
    }

    public List<ProxyRedisMeta> getAllPeerMasters() {
        return new ArrayList<>(peerMasters.values());
    }

    private ProxyRedisMeta cloneMasterMeta(ProxyRedisMeta peerMaster) {
        if (null == peerMaster) return null;
        ProxyRedisMeta meta = ProxyRedisMeta.valueof(peerMaster).setProxy(peerMaster.getProxy());
        return meta;
    }

}
