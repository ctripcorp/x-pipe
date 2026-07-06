package com.ctrip.xpipe.redis.console.controller.api.dto;

public class ClusterBeaconRouteItem {

    private String system;
    private String beaconMode;
    private String clusterName;
    private String dcName;
    private String type;
    private String azGroupType;
    private long orgId;
    private boolean activeDc;
    private String beaconName;
    private String beaconHost;

    public ClusterBeaconRouteItem() {
    }

    public String getSystem() {
        return system;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public String getBeaconMode() {
        return beaconMode;
    }

    public void setBeaconMode(String beaconMode) {
        this.beaconMode = beaconMode;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAzGroupType() {
        return azGroupType;
    }

    public void setAzGroupType(String azGroupType) {
        this.azGroupType = azGroupType;
    }

    public long getOrgId() {
        return orgId;
    }

    public void setOrgId(long orgId) {
        this.orgId = orgId;
    }

    public boolean isActiveDc() {
        return activeDc;
    }

    public void setActiveDc(boolean activeDc) {
        this.activeDc = activeDc;
    }

    public String getBeaconName() {
        return beaconName;
    }

    public void setBeaconName(String beaconName) {
        this.beaconName = beaconName;
    }

    public String getBeaconHost() {
        return beaconHost;
    }

    public void setBeaconHost(String beaconHost) {
        this.beaconHost = beaconHost;
    }
}
