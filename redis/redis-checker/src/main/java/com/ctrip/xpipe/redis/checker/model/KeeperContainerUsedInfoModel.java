package com.ctrip.xpipe.redis.checker.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class KeeperContainerUsedInfoModel {

    private String keeperIp;

    private String dcName;

    private long activeInputFlow;

    private long totalInputFlow;

    private long redisUsedMemory;

    private Map<DcClusterShardActive, KeeperUsedInfo> detailInfo;

    private boolean diskAvailable;

    private long diskSize;

    private long diskUsed;

    private String diskType = "default";

    private List<String> overLoadCause = new ArrayList<>();

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

    public Map<DcClusterShardActive, KeeperUsedInfo> getDetailInfo() {
        return detailInfo;
    }

    public KeeperContainerUsedInfoModel setDetailInfo(Map<DcClusterShardActive, KeeperUsedInfo> detailInfo) {
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

    public String getDiskType() {
        return diskType;
    }

    public void setDiskType(String diskType) {
        this.diskType = diskType;
    }

    public List<String> getOverLoadCause() {
        return overLoadCause;
    }

    public void setOverLoadCause(List<String> overLoadCause) {
        this.overLoadCause = overLoadCause;
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
                ", diskType='" + diskType + '\'' +
                ", overLoadCause=" + overLoadCause +
                '}';
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

    public static class KeeperUsedInfo {

        private long peerData;

        private long inputFlow;

        private String keeperIP;


        public KeeperUsedInfo(long peerData, long inputFlow, String keeperIP) {
            this.peerData = peerData;
            this.inputFlow = inputFlow;
            this.keeperIP = keeperIP;
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

        public String getKeeperIP() {
            return keeperIP;
        }

        public void setKeeperIP(String keeperIP) {
            this.keeperIP = keeperIP;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof KeeperUsedInfo)) return false;
            KeeperUsedInfo that = (KeeperUsedInfo) o;
            return getPeerData() == that.getPeerData() && getInputFlow() == that.getInputFlow() && Objects.equals(getKeeperIP(), that.getKeeperIP());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getPeerData(), getInputFlow(), getKeeperIP());
        }

        @Override
        public String toString() {
            return "KeeperContainerInfo{" +
                    "peerData=" + peerData +
                    ", inputFlow=" + inputFlow +
                    ", keeperContainerIP='" + keeperIP + '\'' +
                    '}';
        }
    }

}
