package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import java.util.List;
import java.util.Objects;

public class RegionInfo {

    private String region;
    private String clusterType;
    private String activeAz;
    private List<String> azs;

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

    @Override
    public String toString() {
        return "RegionInfo{" + "region='" + region + '\'' + ", clusterType='" + clusterType + '\'' + ", activeAz='"
            + activeAz + '\'' + ", azs=" + azs + '}';
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
        return Objects.equals(azs, that.azs);
    }

    @Override
    public int hashCode() {
        int result = region != null ? region.hashCode() : 0;
        result = 31 * result + (clusterType != null ? clusterType.hashCode() : 0);
        result = 31 * result + (activeAz != null ? activeAz.hashCode() : 0);
        result = 31 * result + (azs != null ? azs.hashCode() : 0);
        return result;
    }
}
