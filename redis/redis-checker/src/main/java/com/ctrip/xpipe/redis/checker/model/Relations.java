package com.ctrip.xpipe.redis.checker.model;

import java.util.List;

public class Relations {
    private int delayPerDistance = 2000;
    private List<Relation> dcLevel;
    private List<ClusterRelations> clusterLevel;
    private List<Relation> regionLevel;

    public List<Relation> getDcLevel() {
        return dcLevel;
    }

    public Relations setDcLevel(List<Relation> dcLevel) {
        this.dcLevel = dcLevel;
        return this;
    }

    public List<ClusterRelations> getClusterLevel() {
        return clusterLevel;
    }

    public Relations setClusterLevel(List<ClusterRelations> clusterLevel) {
        this.clusterLevel = clusterLevel;
        return this;
    }

    public List<Relation> getRegionLevel() {
        return regionLevel;
    }

    public Relations setRegionLevel(List<Relation> regionLevel) {
        this.regionLevel = regionLevel;
        return this;
    }

    public int getDelayPerDistance() {
        return delayPerDistance;
    }

    public Relations setDelayPerDistance(int delayPerDistance) {
        this.delayPerDistance = delayPerDistance;
        return this;
    }
}
