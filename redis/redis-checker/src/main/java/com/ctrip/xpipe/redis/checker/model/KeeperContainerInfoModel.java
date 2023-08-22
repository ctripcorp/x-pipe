package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;

public class KeeperContainerInfoModel {

    private String keeperIp;

    private long totalInputFlow;

    private long totalRedisUsedMemory;

    private Map<DcClusterShard, Pair<Long, Long>> detailInfo;

    public String getKeeperIp() {
        return keeperIp;
    }

    public KeeperContainerInfoModel setKeeperIp(String keeperIp) {
        this.keeperIp = keeperIp;
        return this;
    }

    public long getTotalInputFlow() {
        return totalInputFlow;
    }

    public KeeperContainerInfoModel setTotalInputFlow(long totalInputFlow) {
        this.totalInputFlow = totalInputFlow;
        return this;
    }

    public long getTotalRedisUsedMemory() {
        return totalRedisUsedMemory;
    }

    public KeeperContainerInfoModel setTotalRedisUsedMemory(long totalRedisUsedMemory) {
        this.totalRedisUsedMemory = totalRedisUsedMemory;
        return this;
    }

    public Map<DcClusterShard, Pair<Long, Long>> getDetailInfo() {
        return detailInfo;
    }

    public KeeperContainerInfoModel setDetailInfo(Map<DcClusterShard, Pair<Long, Long>> detailInfo) {
        this.detailInfo = detailInfo;
        return this;
    }

    @Override
    public String toString() {
        return "KeeperContainerInfoModel{" +
                "keeperIp='" + keeperIp + '\'' +
                ", totalInputFlow=" + totalInputFlow +
                ", totalRedisUsedMemory=" + totalRedisUsedMemory +
                ", detailInfo=" + detailInfo +
                '}';
    }
}
