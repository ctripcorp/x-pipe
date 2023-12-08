package com.ctrip.xpipe.redis.checker.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KeeperContainerUsedInfoModel {

    private String keeperIp;

    private String dcName;

    private long activeInputFlow;

    private long totalInputFlow;

    private long redisUsedMemory;

    private Map<DcClusterShardActive, KeeperContainerUsedInfo> detailInfo;

    private boolean diskAvailable;

    private long diskSize;

    private long diskUsed;

    public KeeperContainerUsedInfoModel() {
    }

    public KeeperContainerUsedInfoModel(String keeperIp, String dcName, long activeInputFlow, long redisUsedMemory) {
        this.keeperIp = keeperIp;
        this.dcName = dcName;
        this.activeInputFlow = activeInputFlow;
        this.redisUsedMemory = redisUsedMemory;
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

    public long getActiveInputFlow() {
        return activeInputFlow;
    }

    public KeeperContainerUsedInfoModel setActiveInputFlow(long activeInputFlow) {
        this.activeInputFlow = activeInputFlow;
        return this;
    }

    public long getTotalInputFlow() {
        return totalInputFlow;
    }

    public KeeperContainerUsedInfoModel setTotalInputFlow(long totalInputFlow) {
        this.totalInputFlow = totalInputFlow;
        return this;
    }

    public long getRedisUsedMemory() {
        return redisUsedMemory;
    }

    public KeeperContainerUsedInfoModel setRedisUsedMemory(long redisUsedMemory) {
        this.redisUsedMemory = redisUsedMemory;
        return this;
    }

    public Map<DcClusterShardActive, KeeperContainerUsedInfo> getDetailInfo() {
        return detailInfo;
    }

    public KeeperContainerUsedInfoModel setDetailInfo(Map<DcClusterShardActive, KeeperContainerUsedInfo> detailInfo) {
        this.detailInfo = detailInfo;
        return this;
    }

    public boolean isDiskAvailable() {
        return diskAvailable;
    }

    public KeeperContainerUsedInfoModel setDiskAvailable(boolean diskAvailable) {
        this.diskAvailable = diskAvailable;
        return this;
    }

    public long getDiskSize() {
        return diskSize;
    }

    public KeeperContainerUsedInfoModel setDiskSize(long diskSize) {
        this.diskSize = diskSize;
        return this;
    }

    public long getDiskUsed() {
        return diskUsed;
    }

    public KeeperContainerUsedInfoModel setDiskUsed(long diskUsed) {
        this.diskUsed = diskUsed;
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
                ", activeInputFlow=" + activeInputFlow +
                ", totalInputFlow=" + totalInputFlow +
                ", redisUsedMemory=" + redisUsedMemory +
                ", detailInfo=" + detailInfo +
                ", diskAvailable=" + diskAvailable +
                ", diskSize=" + diskSize +
                ", diskUsed=" + diskUsed +
                '}';
    }

    public static class KeeperContainerUsedInfo {

        private long peerData;

        private long inputFlow;

        private String keeperContainerIP;

        public KeeperContainerUsedInfo(long peerData, long inputFlow, String keeperContainerIP) {
            this.peerData = peerData;
            this.inputFlow = inputFlow;
            this.keeperContainerIP = keeperContainerIP;
        }

        public long getPeerData() {
            return peerData;
        }

        public void setPeerData(long peerData) {
            this.peerData = peerData;
        }

        public long getInputFlow() {
            return inputFlow;
        }

        public void setInputFlow(long inputFlow) {
            this.inputFlow = inputFlow;
        }

        public String getKeeperContainerIP() {
            return keeperContainerIP;
        }

        public void setKeeperContainerIP(String keeperContainerIP) {
            this.keeperContainerIP = keeperContainerIP;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof KeeperContainerUsedInfo)) return false;
            KeeperContainerUsedInfo that = (KeeperContainerUsedInfo) o;
            return getPeerData() == that.getPeerData() && getInputFlow() == that.getInputFlow() && Objects.equals(getKeeperContainerIP(), that.getKeeperContainerIP());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getPeerData(), getInputFlow(), getKeeperContainerIP());
        }

        @Override
        public String toString() {
            return "KeeperContainerInfo{" +
                    "peerData=" + peerData +
                    ", inputFlow=" + inputFlow +
                    ", keeperContainerIP='" + keeperContainerIP + '\'' +
                    '}';
        }
    }

}
