package com.ctrip.xpipe.redis.console.dto;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionInfo;
import org.springframework.util.CollectionUtils;

public class ClusterCreateDTO {

    private String clusterName;
    private String clusterType;
    private String activeAz;
    private String description;
    private String orgName;
    private String adminEmails;

    public ClusterCreateDTO() {
    }

    public ClusterCreateDTO(ClusterCreateDTO createDTO) {
        this.clusterName = createDTO.getClusterName();
        this.clusterType = createDTO.getClusterType();
        this.activeAz = createDTO.getActiveAz();
        this.description = createDTO.getDescription();
        this.orgName = createDTO.getOrgName();
        this.adminEmails = createDTO.getAdminEmails();
    }

    public ClusterCreateDTO(String clusterName, String clusterType, String activeAz, String description,
        String orgName, String adminEmails) {
        this.clusterName = clusterName;
        this.clusterType = clusterType;
        this.activeAz = activeAz;
        this.description = description;
        this.orgName = orgName;
        this.adminEmails = adminEmails;
    }

    public ClusterCreateDTO(ClusterCreateInfo clusterCreateInfo, String orgName) {
        this.clusterName = clusterCreateInfo.getClusterName();
        this.clusterType = clusterCreateInfo.getClusterType();
        if (CollectionUtils.isEmpty(clusterCreateInfo.getDcs())) {
            for (RegionInfo region : clusterCreateInfo.getRegions()) {
                if (this.clusterType.equalsIgnoreCase(region.getClusterType())) {
                    this.activeAz = region.getActiveAz();
                    break;
                }
            }
        } else {
            this.activeAz = clusterCreateInfo.getDcs().get(0);
        }
        this.description = clusterCreateInfo.getDesc();
        this.orgName = orgName;
        this.adminEmails= clusterCreateInfo.getClusterAdminEmails();
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

    public String getAdminEmails() {
        return adminEmails;
    }

    public void setAdminEmails(String adminEmails) {
        this.adminEmails = adminEmails;
    }

    public static ClusterCreateDTO.Builder builder() {
        return new ClusterCreateDTO.Builder();
    }

    public static final class Builder {
        private String clusterName;
        private String clusterType;
        private String activeAz;
        private String description;
        private String orgName;
        private String adminEmails;

        public Builder() {
        }

        public Builder clusterName(String val) {
            clusterName = val;
            return this;
        }

        public Builder clusterType(String val) {
            clusterType = val;
            return this;
        }

        public Builder activeAz(String val) {
            activeAz = val;
            return this;
        }

        public Builder description(String val) {
            description = val;
            return this;
        }

        public Builder orgName(String val) {
            orgName = val;
            return this;
        }

        public Builder adminEmails(String val) {
            adminEmails = val;
            return this;
        }

        public ClusterCreateDTO build() {
            return new ClusterCreateDTO(this.clusterName, this.clusterType, this.activeAz,
                this.description, this.orgName, this.adminEmails);
        }
    }
}
