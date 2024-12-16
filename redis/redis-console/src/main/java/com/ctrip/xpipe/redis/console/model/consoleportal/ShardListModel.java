package com.ctrip.xpipe.redis.console.model.consoleportal;

import java.util.ArrayList;
import java.util.List;

public class ShardListModel extends AbstractClusterModel {

    private Long activedcId;

    private String shardName;

    private Long shardId;

    private String clusterType;

    private List<String> dcNames = new ArrayList<>();

    public String getShardName() {
        return shardName;
    }

    public ShardListModel setShardName(String shardName) {
        this.shardName = shardName;
        return this;
    }

    public Long getShardId() {
        return shardId;
    }

    public ShardListModel setShardId(Long shardId) {
        this.shardId = shardId;
        return this;
    }

    public List<String> getDcNames() {
        return dcNames;
    }

    public ShardListModel addDc(String dcName) {
        this.dcNames.add(dcName);
        return this;
    }

    public Long getActivedcId() {
        return activedcId;
    }

    public ShardListModel setActivedcId(Long activedcId) {
        this.activedcId = activedcId;
        return this;
    }

}
