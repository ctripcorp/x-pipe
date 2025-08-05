package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import java.util.Objects;

public class ClusterActiveDcKey {
    private String cluster;

    private String activeDc;

    public ClusterActiveDcKey(String cluster, String activeDc) {
        this.cluster = cluster;
        this.activeDc = activeDc;
    }

    public String getCluster() {
        return cluster;
    }

    public String getActiveDc() {
        return activeDc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterActiveDcKey that = (ClusterActiveDcKey) o;
        return Objects.equals(cluster, that.cluster) &&
                Objects.equals(activeDc, that.activeDc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, activeDc);
    }

    @Override
    public String toString() {
        return cluster + ":" + activeDc;
    }
}
