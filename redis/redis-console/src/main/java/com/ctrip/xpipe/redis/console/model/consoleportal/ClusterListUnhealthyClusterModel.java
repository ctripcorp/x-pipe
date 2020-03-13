package com.ctrip.xpipe.redis.console.model.consoleportal;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jan 31, 2018
 */
public class ClusterListUnhealthyClusterModel extends AbstractClusterModel {

    private Long activedcId;

    private List<String> messages;

    private int unhealthyShardsCnt;

    private int unhealthyRedisCnt;

    public ClusterListUnhealthyClusterModel(String clusterName) {
        super(clusterName);
    }

    public ClusterListUnhealthyClusterModel() {
    }

    public Long getActivedcId() {
        return activedcId;
    }

    public ClusterListUnhealthyClusterModel setActivedcId(Long activedcId) {
        this.activedcId = activedcId;
        return this;
    }

    public List<String> getMessages() {
        return messages;
    }

    public ClusterListUnhealthyClusterModel setMessages(List<String> messages) {
        this.messages = messages;
        return this;
    }

    public int getUnhealthyShardsCnt() {
        return unhealthyShardsCnt;
    }

    public ClusterListUnhealthyClusterModel setUnhealthyShardsCnt(int unhealthyShardsCnt) {
        this.unhealthyShardsCnt = unhealthyShardsCnt;
        return this;
    }

    public int getUnhealthyRedisCnt() {
        return unhealthyRedisCnt;
    }

    public ClusterListUnhealthyClusterModel setUnhealthyRedisCnt(int unhealthyRedisCnt) {
        this.unhealthyRedisCnt = unhealthyRedisCnt;
        return this;
    }
}
