package com.ctrip.xpipe.redis.console.model;

import java.io.Serializable;

public class ClusterConfigModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private String clusterName;
    private Boolean  value;

    public ClusterConfigModel() {
    }

    public ClusterConfigModel(String clusterName, Boolean value) {
        this.clusterName = clusterName;
        this.value = value;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Boolean getValue() {
        return value;
    }

    public void setValue(Boolean value) {
        this.value = value;
    }
}
