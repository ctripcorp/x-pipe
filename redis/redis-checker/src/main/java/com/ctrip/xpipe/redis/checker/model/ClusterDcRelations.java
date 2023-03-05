package com.ctrip.xpipe.redis.checker.model;

import java.util.List;

public class ClusterDcRelations {
    private String clusterName;
    private List<DcRelation> relations;

    public String getClusterName() {
        return clusterName;
    }

    public ClusterDcRelations setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public List<DcRelation> getRelations() {
        return relations;
    }

    public ClusterDcRelations setRelations(List<DcRelation> relations) {
        this.relations = relations;
        return this;
    }
}
