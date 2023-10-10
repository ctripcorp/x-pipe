package com.ctrip.xpipe.redis.console.dto;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionInfo;

import java.util.List;

public class AzGroupDTO {

    private String region;
    private String clusterType;
    private String activeAz;
    private List<String> azs;

    public AzGroupDTO() {
    }

    public AzGroupDTO(String region, String clusterType, String activeAz, List<String> azs) {
        this.region = region;
        this.clusterType = clusterType;
        this.activeAz = activeAz;
        this.azs = azs;
    }

    public AzGroupDTO(RegionInfo regionInfo) {
        this.region = regionInfo.getRegion();
        this.clusterType = regionInfo.getClusterType();
        this.activeAz = regionInfo.getActiveAz();
        this.azs = regionInfo.getAzs();
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
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

    public List<String> getAzs() {
        return azs;
    }

    public void setAzs(List<String> azs) {
        this.azs = azs;
    }

    public static AzGroupDTO.Builder builder() {
        return new AzGroupDTO.Builder();
    }

    public static final class Builder {
        private String region;
        private String clusterType;
        private String activeAz;
        private List<String> azs;

        public Builder() {
        }

        public Builder region(String val) {
            region = val;
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

        public Builder azs(List<String> val) {
            azs = val;
            return this;
        }

        public AzGroupDTO build() {
            return new AzGroupDTO(this.region, this.clusterType, this.activeAz, this.azs);
        }
    }
}
