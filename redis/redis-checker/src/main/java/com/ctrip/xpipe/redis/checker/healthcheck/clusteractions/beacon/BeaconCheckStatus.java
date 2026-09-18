package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

public enum BeaconCheckStatus {
    SERVICE_NOT_FOUND("Service not found"),
    SYSTEM_NOT_FOUND("System not found"),
    CLUSTER_NOT_FOUND("Cluster not found"),
    INCONSISTENCY("Inconsistency"),
    CONSISTENCY("Consistency"),
    UNKNOWN("Unknown"),
    ERROR("Error");

    private final String description;

    BeaconCheckStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
