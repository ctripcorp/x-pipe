package com.ctrip.xpipe.redis.checker.model;

import java.util.List;

public class DcsRelations {
    private int delayPerDistance = 2000;
    private List<DcRelation> dcLevel;
    private List<ClusterDcRelations> clusterLevel;

    public List<DcRelation> getDcLevel() {
        return dcLevel;
    }

    public DcsRelations setDcLevel(List<DcRelation> dcLevel) {
        this.dcLevel = dcLevel;
        return this;
    }

    public List<ClusterDcRelations> getClusterLevel() {
        return clusterLevel;
    }

    public DcsRelations setClusterLevel(List<ClusterDcRelations> clusterLevel) {
        this.clusterLevel = clusterLevel;
        return this;
    }

    public int getDelayPerDistance() {
        return delayPerDistance;
    }

    public void setDelayPerDistance(int delayPerDistance) {
        this.delayPerDistance = delayPerDistance;
    }
}
