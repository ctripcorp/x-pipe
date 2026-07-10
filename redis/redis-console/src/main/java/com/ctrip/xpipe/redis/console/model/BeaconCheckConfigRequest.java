package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.utils.StringUtil;

import java.util.List;

public class BeaconCheckConfigRequest {

    private String clusterName;

    private String dc;

    private List<String> shards;

    public void validate() {
        if (StringUtil.isEmpty(clusterName)) {
            throw new IllegalArgumentException("clusterName can not be empty");
        }
        if (StringUtil.isEmpty(dc)) {
            throw new IllegalArgumentException("dc can not be empty");
        }
        if (shards == null || shards.isEmpty()) {
            throw new IllegalArgumentException("shards can not be empty");
        }
        for (String shard : shards) {
            if (StringUtil.isEmpty(shard)) {
                throw new IllegalArgumentException("shard name can not be empty");
            }
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public BeaconCheckConfigRequest setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getDc() {
        return dc;
    }

    public BeaconCheckConfigRequest setDc(String dc) {
        this.dc = dc;
        return this;
    }

    public List<String> getShards() {
        return shards;
    }

    public BeaconCheckConfigRequest setShards(List<String> shards) {
        this.shards = shards;
        return this;
    }
}
