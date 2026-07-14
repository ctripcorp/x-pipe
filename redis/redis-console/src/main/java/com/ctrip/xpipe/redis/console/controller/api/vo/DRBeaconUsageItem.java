package com.ctrip.xpipe.redis.console.controller.api.vo;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;

import java.util.List;

public class DRBeaconUsageItem {

    private String system;
    private String beaconMode;
    private long orgId;
    private String beaconName;
    private String beaconHost;
    private int clusterCount;
    private int shardCount;
    private List<ClusterShardInfo> clusters;

    public DRBeaconUsageItem() {
    }

    public DRBeaconUsageItem(BeaconSystem beaconSystem, long orgId, MonitorService service,
                              int clusterCount, int shardCount, List<ClusterShardInfo> clusters) {
        this.system = beaconSystem.getSystemName();
        this.beaconMode = BeaconRouteType.DR.name();
        this.orgId = orgId;
        this.beaconName = service.getName();
        this.beaconHost = service.getHost();
        this.clusterCount = clusterCount;
        this.shardCount = shardCount;
        this.clusters = clusters;
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

    public long getOrgId() {
        return orgId;
    }

    public void setOrgId(long orgId) {
        this.orgId = orgId;
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

    public int getClusterCount() {
        return clusterCount;
    }

    public void setClusterCount(int clusterCount) {
        this.clusterCount = clusterCount;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    public List<ClusterShardInfo> getClusters() {
        return clusters;
    }

    public void setClusters(List<ClusterShardInfo> clusters) {
        this.clusters = clusters;
    }
}
