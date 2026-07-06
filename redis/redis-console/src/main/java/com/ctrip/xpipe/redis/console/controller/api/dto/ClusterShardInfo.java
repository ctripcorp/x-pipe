package com.ctrip.xpipe.redis.console.controller.api.dto;

public class ClusterShardInfo {

    private String name;
    private int shardCount;

    public ClusterShardInfo() {
    }

    public ClusterShardInfo(String name, int shardCount) {
        this.name = name;
        this.shardCount = shardCount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }
}
