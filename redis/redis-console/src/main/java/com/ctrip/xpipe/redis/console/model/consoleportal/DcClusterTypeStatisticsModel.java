package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import java.util.ArrayList;
import java.util.List;

public class DcClusterTypeStatisticsModel {

    private String dcName;
    private String clusterType;
    private List<ClusterMeta> typeClusters = new ArrayList<>();
    private int clusterCount;
    private int redisCount;
    private int keeperCount;
    private int keeperContainerCount;
    private int clusterInActiveDcCount;

    public DcClusterTypeStatisticsModel() {
    }

    public DcClusterTypeStatisticsModel(String dc, String clusterType, List<ClusterMeta> typeClusters) {
        this.dcName = dc;
        this.clusterType = clusterType;
        this.typeClusters = typeClusters;
    }

    public DcClusterTypeStatisticsModel(DcClusterTypeStatisticsModel model) {
        this.dcName = model.dcName;
        this.clusterType = model.clusterType;
        this.typeClusters = model.typeClusters;
        this.clusterCount = model.clusterCount;
        this.redisCount = model.redisCount;
        this.keeperCount = model.keeperCount;
        this.keeperContainerCount = model.keeperContainerCount;
        this.clusterInActiveDcCount = model.clusterInActiveDcCount;
    }


    public String getDcName() {
        return dcName;
    }

    public DcClusterTypeStatisticsModel setDcName(String dcName) {
        this.dcName = dcName;
        return this;
    }

    public String getClusterType() {
        return clusterType;
    }

    public DcClusterTypeStatisticsModel setClusterType(String clusterType) {
        this.clusterType = clusterType;
        return this;
    }

    public int getClusterCount() {
        return clusterCount;
    }

    public DcClusterTypeStatisticsModel setClusterCount(int clusterCount) {
        this.clusterCount = clusterCount;
        return this;
    }

    public int getRedisCount() {
        return redisCount;
    }

    public DcClusterTypeStatisticsModel setRedisCount(int redisCount) {
        this.redisCount = redisCount;return this;
    }

    public int getKeeperCount() {
        return keeperCount;
    }

    public DcClusterTypeStatisticsModel setKeeperCount(int keeperCount) {
        this.keeperCount = keeperCount;
        return this;
    }

    public int getKeeperContainerCount() {
        return keeperContainerCount;
    }

    public DcClusterTypeStatisticsModel setKeeperContainerCount(int keeperContainerCount) {
        this.keeperContainerCount = keeperContainerCount;
        return this;
    }

    public int getClusterInActiveDcCount() {
        return clusterInActiveDcCount;
    }

    public DcClusterTypeStatisticsModel setClusterInActiveDcCount(int clusterInActiveDcCount) {
        this.clusterInActiveDcCount = clusterInActiveDcCount;
        return this;
    }

    public void analyse() {
        clusterCount = typeClusters.size();
        for (ClusterMeta clusterMeta : typeClusters) {
            if (dcName.equals(clusterMeta.getActiveDc()))
                clusterInActiveDcCount++;

            for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                if (-redisCount <= Integer.MIN_VALUE || -redisCount - shardMeta.getRedises().size() <= Integer.MIN_VALUE) {
                    throw new XpipeRuntimeException(String.format("redis numbers overflow: %d", redisCount));
                } else if (-keeperCount <= Integer.MIN_VALUE || -keeperCount - shardMeta.getKeepers().size() <= Integer.MIN_VALUE) {
                    throw new XpipeRuntimeException(String.format("keeper numbers overflow: %d", keeperCount));
                } else {
                    redisCount += shardMeta.getRedises().size();
                    keeperCount += shardMeta.getKeepers().size();
                }
            }
        }
    }

    public void addCounts(DcClusterTypeStatisticsModel other) {
        this.clusterCount += other.clusterCount;
        this.redisCount += other.redisCount;
        this.keeperCount += other.keeperCount;
        this.clusterInActiveDcCount += other.clusterInActiveDcCount;
        this.keeperContainerCount += other.keeperContainerCount;
    }
}
