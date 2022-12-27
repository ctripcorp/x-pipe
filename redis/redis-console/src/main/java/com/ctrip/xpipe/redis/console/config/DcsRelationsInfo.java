package com.ctrip.xpipe.redis.console.config;

import java.util.List;

public class DcsRelationsInfo {
    private int healthyDelayPerDistance;
    private List<DcRelation> relations;

    public int getHealthyDelayPerDistance() {
        return healthyDelayPerDistance;
    }

    public DcsRelationsInfo setHealthyDelayPerDistance(int healthyDelayPerDistance) {
        this.healthyDelayPerDistance = healthyDelayPerDistance;
        return this;
    }

    public List<DcRelation> getRelations() {
        return relations;
    }

    public DcsRelationsInfo setRelations(List<DcRelation> relations) {
        this.relations = relations;
        return this;
    }
}
