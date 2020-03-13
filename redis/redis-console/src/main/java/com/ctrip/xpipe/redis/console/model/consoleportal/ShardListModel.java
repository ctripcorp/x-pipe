package com.ctrip.xpipe.redis.console.model.consoleportal;

public class ShardListModel extends AbstractClusterModel {

    private String shardName;

    private String dcName;

    public String getShardName() {
        return shardName;
    }

    public ShardListModel setShardName(String shardName) {
        this.shardName = shardName;
        return this;
    }

    public String getDcName() {
        return dcName;
    }

    public ShardListModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }
}
