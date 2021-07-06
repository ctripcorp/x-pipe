package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CurrentCRDTShardMeta extends AbstractCurrentShardMeta {

    RedisMeta currentMaster;

    Map<String, RedisProxyMeta> peerMasters = new ConcurrentHashMap<>();

    public CurrentCRDTShardMeta(@JsonProperty("clusterId") String clusterId, @JsonProperty("shardId") String shardId) {
        super(clusterId, shardId);
    }

    public void setCurrentMaster(RedisMeta master) {
        if (null == master) return;
        this.currentMaster = RedisProxyMeta.valueof(master);
    }

    public RedisMeta getCurrentMaster() {
        return cloneMasterMeta((RedisProxyMeta) currentMaster);
    }

    public void setPeerMaster(String dcId, RedisProxyMeta peerMaster) {
        if (null == peerMaster) return;

        peerMasters.put(dcId.toLowerCase(), cloneMasterMeta(peerMaster));
    }

    public RedisProxyMeta getPeerMaster(String dcId) {
        RedisProxyMeta peerMaster = peerMasters.get(dcId.toLowerCase());
        return cloneMasterMeta(peerMaster);
    }

    public void removePeerMaster(String dcId) {
        peerMasters.remove(dcId.toLowerCase());
    }

    public Set<String> getUpstreamPeerDcs() {
        return new HashSet<>(peerMasters.keySet());
    }

    public List<RedisProxyMeta> getAllPeerMasters() {
        return new ArrayList<>(peerMasters.values());
    }

    private RedisProxyMeta cloneMasterMeta(RedisProxyMeta peerMaster) {
        if (null == peerMaster) return null;
        RedisProxyMeta meta = RedisProxyMeta.valueof(peerMaster).setProxy(peerMaster.getProxy());
        return meta;
    }

}
