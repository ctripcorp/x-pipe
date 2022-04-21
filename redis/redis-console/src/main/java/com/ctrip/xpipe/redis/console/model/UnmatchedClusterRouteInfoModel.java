package com.ctrip.xpipe.redis.console.model;

public class UnmatchedClusterRouteInfoModel {
    private String clusterName;

    private String srcDcName;

    private String dstDcName;

    private long usedRouteId;

    private long chooseRouteId;

    public UnmatchedClusterRouteInfoModel() {
    }

    public UnmatchedClusterRouteInfoModel(String clusterName, String srcDcName, String dstDcName, long usedRouteId, long chooseRouteId) {
        this.clusterName = clusterName;
        this.srcDcName = srcDcName;
        this.dstDcName = dstDcName;
        this.usedRouteId = usedRouteId;
        this.chooseRouteId = chooseRouteId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public UnmatchedClusterRouteInfoModel setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public UnmatchedClusterRouteInfoModel setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
        return this;
    }

    public String getDstDcName() {
        return dstDcName;
    }

    public UnmatchedClusterRouteInfoModel setDstDcName(String dstDcName) {
        this.dstDcName = dstDcName;
        return this;
    }

    public long getUsedRouteId() {
        return usedRouteId;
    }

    public UnmatchedClusterRouteInfoModel setUsedRouteId(long usedRouteId) {
        this.usedRouteId = usedRouteId;
        return this;
    }

    public long getChooseRouteId() {
        return chooseRouteId;
    }

    public UnmatchedClusterRouteInfoModel setChooseRouteId(long chooseRouteId) {
        this.chooseRouteId = chooseRouteId;
        return this;
    }

    @Override
    public String toString() {
        return "UnmatchedClusterRouteInfoModel{" +
                "clustername='" + clusterName + '\'' +
                ", srcDcName='" + srcDcName + '\'' +
                ", dstDcName='" + dstDcName + '\'' +
                ", usedRouteId=" + usedRouteId +
                ", chooseRouteId=" + chooseRouteId +
                '}';
    }
}
