package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

public class RegionInfo {

    private String region;
    private String clusterType;
    private String activeAz;
    private List<String> azs;

    /** Regions contained by the AzGroup (read API); CRedis may ignore. */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<String> regions;

    public RegionInfo() {
    }

    public RegionInfo(String region, String clusterType, String activeAz, List<String> azs) {
        this.region = region;
        this.clusterType = clusterType;
        this.activeAz = activeAz;
        this.azs = azs;
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

    public List<String> getRegions() {
        return regions;
    }

    public void setRegions(List<String> regions) {
        this.regions = regions;
    }

    @Override
    public String toString() {
        return "RegionInfo{" + "region='" + region + '\'' + ", clusterType='" + clusterType + '\'' + ", activeAz='"
            + activeAz + '\'' + ", azs=" + azs + ", regions=" + regions + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RegionInfo that = (RegionInfo)o;

        if (!Objects.equals(region, that.region))
            return false;
        if (!Objects.equals(clusterType, that.clusterType))
            return false;
        if (!Objects.equals(activeAz, that.activeAz))
            return false;
        if (!Objects.equals(azs, that.azs))
            return false;
        return Objects.equals(regions, that.regions);
    }

    @Override
    public int hashCode() {
        int result = region != null ? region.hashCode() : 0;
        result = 31 * result + (clusterType != null ? clusterType.hashCode() : 0);
        result = 31 * result + (activeAz != null ? activeAz.hashCode() : 0);
        result = 31 * result + (azs != null ? azs.hashCode() : 0);
        result = 31 * result + (regions != null ? regions.hashCode() : 0);
        return result;
    }
}
