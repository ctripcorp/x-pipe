package com.ctrip.xpipe.redis.console.controller.api.dto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;

import java.util.List;

public class DRClusterBeaconRouteItem {

    private String system;
    private String beaconMode;
    private String clusterName;
    private String type;
    private String azGroupType;
    private long orgId;
    private String activeDc;
    private List<String> dcs;
    private String beaconName;
    private String beaconHost;

    public DRClusterBeaconRouteItem() {
    }

    public DRClusterBeaconRouteItem(BeaconSystem beaconSystem, ClusterMeta clusterMeta,
                                     List<String> dcs, MonitorService monitorService) {
        this.system = beaconSystem.getSystemName();
        this.beaconMode = BeaconRouteType.DR.name();
        this.clusterName = clusterMeta.getId();
        this.type = clusterMeta.getType();
        this.azGroupType = clusterMeta.getAzGroupType();
        this.orgId = clusterMeta.getOrgId();
        this.activeDc = clusterMeta.getActiveDc() != null ? clusterMeta.getActiveDc().toUpperCase() : null;
        this.dcs = dcs;
        this.beaconName = monitorService != null ? monitorService.getName() : null;
        this.beaconHost = monitorService != null ? monitorService.getHost() : null;
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

    public String getActiveDc() {
        return activeDc;
    }

    public void setActiveDc(String activeDc) {
        this.activeDc = activeDc;
    }

    public List<String> getDcs() {
        return dcs;
    }

    public void setDcs(List<String> dcs) {
        this.dcs = dcs;
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
