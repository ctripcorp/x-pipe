package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.*;

public class KeeperContainerUsedInfoModel {

    private String keeperIp;

    private String dcName;

    private long activeInputFlow;

    private long totalInputFlow;

    private long inputFlowStandard;

    private long activeRedisUsedMemory;

    private long totalRedisUsedMemory;

    private long redisUsedMemoryStandard;

    private int activeKeeperCount;

    private int totalKeeperCount;

    private Map<DcClusterShardActive, KeeperUsedInfo> detailInfo;

    private boolean keeperContainerActive;

    private boolean diskAvailable;

    private long diskSize;

    private long diskUsed;

    private String diskType = "default";

    public KeeperContainerUsedInfoModel() {
    }

    public KeeperContainerUsedInfoModel(String keeperIp, String dcName, long activeInputFlow, long activeRedisUsedMemory) {
        this.keeperIp = keeperIp;
        this.dcName = dcName;
        this.activeInputFlow = activeInputFlow;
        this.activeRedisUsedMemory = activeRedisUsedMemory;
    }

    public KeeperContainerUsedInfoModel(KeeperContainerUsedInfoModel model, Map.Entry<DcClusterShardActive, KeeperUsedInfo> dcClusterShard) {
        this.keeperIp = model.getKeeperIp();
        this.dcName = model.getDcName();
        this.activeInputFlow = model.getActiveInputFlow() + dcClusterShard.getValue().getInputFlow();
        this.totalInputFlow = model.getTotalInputFlow() + dcClusterShard.getValue().getInputFlow();
        this.inputFlowStandard = model.getInputFlowStandard();
        this.activeRedisUsedMemory = model.activeRedisUsedMemory + dcClusterShard.getValue().getPeerData();
        this.totalRedisUsedMemory = model.getTotalRedisUsedMemory() + dcClusterShard.getValue().getPeerData();
        this.redisUsedMemoryStandard = model.getRedisUsedMemoryStandard();
        this.activeKeeperCount = model.getActiveKeeperCount() + 1;
        this.totalKeeperCount = model.getTotalKeeperCount() + 1;
        this.detailInfo = model.detailInfo;
        this.detailInfo.put(dcClusterShard.getKey(), dcClusterShard.getValue());
        this.keeperContainerActive = model.isKeeperContainerActive();
        this.diskAvailable = model.isDiskAvailable();
        this.diskSize = model.getDiskSize();
        this.diskUsed = model.getDiskUsed();
        this.diskType = model.getDiskType();
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

    public long getActiveRedisUsedMemory() {
        return activeRedisUsedMemory;
    }

    public KeeperContainerUsedInfoModel setActiveRedisUsedMemory(long activeRedisUsedMemory) {
        this.activeRedisUsedMemory = activeRedisUsedMemory;
        return this;
    }


    public long getTotalRedisUsedMemory() {
        return totalRedisUsedMemory;
    }

    public KeeperContainerUsedInfoModel setTotalRedisUsedMemory(long totalRedisUsedMemory) {
        this.totalRedisUsedMemory = totalRedisUsedMemory;
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

    public int getActiveKeeperCount() {
        return activeKeeperCount;
    }

    public KeeperContainerUsedInfoModel setActiveKeeperCount(int activeKeeperCount) {
        this.activeKeeperCount = activeKeeperCount;
        return this;
    }

    public int getTotalKeeperCount() {
        return totalKeeperCount;
    }

    public KeeperContainerUsedInfoModel setTotalKeeperCount(int totalKeeperCount) {
        this.totalKeeperCount = totalKeeperCount;
        return this;
    }

    public long getInputFlowStandard() {
        return inputFlowStandard;
    }

    public void setInputFlowStandard(long inputFlowStandard) {
        this.inputFlowStandard = inputFlowStandard;
    }

    public long getRedisUsedMemoryStandard() {
        return redisUsedMemoryStandard;
    }

    public void setRedisUsedMemoryStandard(long redisUsedMemoryStandard) {
        this.redisUsedMemoryStandard = redisUsedMemoryStandard;
    }

    public boolean isKeeperContainerActive() {
        return keeperContainerActive;
    }

    public KeeperContainerUsedInfoModel setKeeperContainerActive(boolean keeperContainerActive) {
        this.keeperContainerActive = keeperContainerActive;
        return this;
    }

    @Override
    public String toString() {
        return "KeeperContainerUsedInfoModel{" +
                "keeperIp='" + keeperIp + '\'' +
                ", dcName='" + dcName + '\'' +
                ", activeInputFlow=" + activeInputFlow +
                ", totalInputFlow=" + totalInputFlow +
                ", inputFlowStandard=" + inputFlowStandard +
                ", activeRedisUsedMemory=" + activeRedisUsedMemory +
                ", totalRedisUsedMemory=" + totalRedisUsedMemory +
                ", redisUsedMemoryStandard=" + redisUsedMemoryStandard +
                ", activeKeeperCount=" + activeKeeperCount +
                ", totalKeeperCount=" + totalKeeperCount +
                ", detailInfo=" + detailInfo +
                ", keeperContainerActive=" + keeperContainerActive +
                ", diskAvailable=" + diskAvailable +
                ", diskSize=" + diskSize +
                ", diskUsed=" + diskUsed +
                ", diskType='" + diskType + '\'' +
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

        public KeeperUsedInfo() {
        }

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

    @VisibleForTesting
    public KeeperContainerUsedInfoModel createKeeper(String clusterId, String shardId, boolean active, long inputFlow, long redisUsedMemory){
        if (this.detailInfo == null) this.detailInfo = new HashMap<>();
        detailInfo.put(new DcClusterShardActive(this.dcName, clusterId, shardId, active), new KeeperUsedInfo(redisUsedMemory, inputFlow, this.keeperIp));
        this.setDetailInfo(detailInfo);
        return this;
    }

}
