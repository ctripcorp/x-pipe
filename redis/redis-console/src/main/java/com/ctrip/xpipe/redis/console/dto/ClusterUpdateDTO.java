package com.ctrip.xpipe.redis.console.dto;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionInfo;

import java.util.List;

public class ClusterUpdateDTO {

    private String clusterName;
    private String clusterType;
    private String description;
    private Long orgId;
    private String adminEmails;
    private List<RegionInfo> regions;

    public ClusterUpdateDTO() {
    }

    public ClusterUpdateDTO(ClusterCreateInfo info, Long orgId) {
        this.clusterName = info.getClusterName();
        this.clusterType = info.getClusterType();
        this.description = info.getDesc();
        this.orgId = orgId;
        this.adminEmails = info.getClusterAdminEmails();
        this.regions = info.getRegions();
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterType() {
        return clusterType;
    }

    public void setClusterType(String clusterType) {
        this.clusterType = clusterType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getOrgId() {
        return orgId;
    }

    public void setOrgId(Long orgId) {
        this.orgId = orgId;
    }

    public String getAdminEmails() {
        return adminEmails;
    }

    public void setAdminEmails(String adminEmails) {
        this.adminEmails = adminEmails;
    }

    public List<RegionInfo> getRegions() {
        return regions;
    }

    public void setRegions(List<RegionInfo> regions) {
        this.regions = regions;
    }
}
