package com.ctrip.xpipe.redis.checker.model;

import java.util.List;

public class ClusterRelations {
    private String clusterName;
    private List<Relation> relations;

    public String getClusterName() {
        return clusterName;
    }

    public ClusterRelations setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public List<Relation> getRelations() {
        return relations;
    }

    public ClusterRelations setRelations(List<Relation> relations) {
        this.relations = relations;
        return this;
    }
}
