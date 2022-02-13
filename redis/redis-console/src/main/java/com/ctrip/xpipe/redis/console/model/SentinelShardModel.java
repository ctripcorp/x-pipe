package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.List;

public class SentinelShardModel {
    private List<HostPort> sentinels;
    private String shardName;
    private HostPort master;

    public SentinelShardModel() {
    }

    public SentinelShardModel(String shardName, HostPort master) {
        this.shardName = shardName;
        this.master = master;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public HostPort getMaster() {
        return master;
    }

    public void setMaster(HostPort master) {
        this.master = master;
    }

    public List<HostPort> getSentinels() {
        return sentinels;
    }

    public SentinelShardModel setSentinels(List<HostPort> sentinels) {
        this.sentinels = sentinels;
        return this;
    }
}
