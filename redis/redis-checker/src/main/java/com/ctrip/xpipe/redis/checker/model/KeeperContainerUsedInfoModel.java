package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;
import java.util.Objects;

public class KeeperContainerUsedInfoModel {

    private String keeperIp;

    private String dcName;

    private long totalInputFlow;

    private long totalRedisUsedMemory;

    private Map<DcClusterShard, Pair<Long, Long>> detailInfo;

    public KeeperContainerUsedInfoModel() {
    }

    public KeeperContainerUsedInfoModel(String keeperIp, String dcName, long totalInputFlow, long totalRedisUsedMemory) {
        this.keeperIp = keeperIp;
        this.dcName = dcName;
        this.totalInputFlow = totalInputFlow;
        this.totalRedisUsedMemory = totalRedisUsedMemory;
    }

    public String getDcName() {
        return dcName;
    }

    public KeeperContainerUsedInfoModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public String getKeeperIp() {
        return keeperIp;
    }

    public KeeperContainerUsedInfoModel setKeeperIp(String keeperIp) {
        this.keeperIp = keeperIp;
        return this;
    }

    public long getTotalInputFlow() {
        return totalInputFlow;
    }

    public KeeperContainerUsedInfoModel setTotalInputFlow(long totalInputFlow) {
        this.totalInputFlow = totalInputFlow;
        return this;
    }

    public long getTotalRedisUsedMemory() {
        return totalRedisUsedMemory;
    }

    public KeeperContainerUsedInfoModel setTotalRedisUsedMemory(long totalRedisUsedMemory) {
        this.totalRedisUsedMemory = totalRedisUsedMemory;
        return this;
    }

    public Map<DcClusterShard, Pair<Long, Long>> getDetailInfo() {
        return detailInfo;
    }

    public KeeperContainerUsedInfoModel setDetailInfo(Map<DcClusterShard, Pair<Long, Long>> detailInfo) {
        this.detailInfo = detailInfo;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeeperContainerUsedInfoModel that = (KeeperContainerUsedInfoModel) o;
        return Objects.equals(keeperIp, that.keeperIp) && Objects.equals(dcName, that.dcName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keeperIp, dcName);
    }

    @Override
    public String toString() {
        return "KeeperContainerUsedInfoModel{" +
                "keeperIp='" + keeperIp + '\'' +
                ", dcName='" + dcName + '\'' +
                ", totalInputFlow=" + totalInputFlow +
                ", totalRedisUsedMemory=" + totalRedisUsedMemory +
                ", detailInfo=" + detailInfo +
                '}';
    }
}
