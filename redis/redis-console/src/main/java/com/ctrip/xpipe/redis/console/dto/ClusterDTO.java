package com.ctrip.xpipe.redis.console.dto;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;

import java.util.List;

public class ClusterDTO {

    private Long clusterId;
    private String clusterName;
    private String clusterType;
    private String activeAz;
    private String description;
    private String orgName;
    private Long cmsOrgId;
    private String adminEmails;
    private List<String> azs;
    private List<AzGroupDTO> azGroups;

    public ClusterDTO() {
    }

    public ClusterDTO(ClusterTbl clusterTbl) {
        this.clusterId = clusterTbl.getId();
        this.clusterName = clusterTbl.getClusterName();
        this.clusterType = clusterTbl.getClusterType();
        this.description = clusterTbl.getClusterDescription();
        this.adminEmails = clusterTbl.getClusterAdminEmails();
    }

    public Long getClusterId() {
        return clusterId;
    }

    public void setClusterId(Long clusterId) {
        this.clusterId = clusterId;
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

    public String getActiveAz() {
        return activeAz;
    }

    public void setActiveAz(String activeAz) {
        this.activeAz = activeAz;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOrgName() {
        return orgName;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public Long getCmsOrgId() {
        return cmsOrgId;
    }

    public void setCmsOrgId(Long cmsOrgId) {
        this.cmsOrgId = cmsOrgId;
    }

    public String getAdminEmails() {
        return adminEmails;
    }

    public void setAdminEmails(String adminEmails) {
        this.adminEmails = adminEmails;
    }

    public List<String> getAzs() {
        return azs;
    }

    public void setAzs(List<String> azs) {
        this.azs = azs;
    }

    public List<AzGroupDTO> getAzGroups() {
        return azGroups;
    }

    public void setAzGroups(List<AzGroupDTO> azGroups) {
        this.azGroups = azGroups;
    }
}
