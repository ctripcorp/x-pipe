package com.ctrip.xpipe.redis.console.model;

import java.util.List;

public class KeeperContainerOverloadStandardModel {

    private long peerDataOverload;

    private long flowOverload;

    private List<DiskTypesEnum> diskTypeEnums;

    public KeeperContainerOverloadStandardModel() {
    }

    public long getPeerDataOverload() {
        return peerDataOverload;
    }

    public KeeperContainerOverloadStandardModel setPeerDataOverload(long peerDataOverload) {
        this.peerDataOverload = peerDataOverload;
        return this;
    }

    public long getFlowOverload() {
        return flowOverload;
    }

    public KeeperContainerOverloadStandardModel setFlowOverload(long flowOverload) {
        this.flowOverload = flowOverload;
        return this;
    }

    public List<DiskTypesEnum> getDiskTypes() {
        return diskTypeEnums;
    }

    public KeeperContainerOverloadStandardModel setDiskTypes(List<DiskTypesEnum> diskTypeEnums) {
        this.diskTypeEnums = diskTypeEnums;
        return this;
    }

    @Override
    public String toString() {
        return "KeeperContainerOverloadStandardModel{" +
                "peerDataOverload=" + peerDataOverload +
                ", flowOverload=" + flowOverload +
                ", diskType=" + diskTypeEnums +
                '}';
    }

    public static class DiskTypesEnum {
        private DiskType diskType;

        private long peerDataOverload;

        private long flowOverload;

        public DiskTypesEnum() {
        }

        public DiskTypesEnum(DiskType diskType, long peerDataOverload, long flowOverload) {
            this.diskType = diskType;
            this.peerDataOverload = peerDataOverload;
            this.flowOverload = flowOverload;
        }

        public DiskType getDiskType() {
            return diskType;
        }

        public void setDiskType(DiskType diskType) {
            this.diskType = diskType;
        }

        public long getPeerDataOverload() {
            return peerDataOverload;
        }

        public void setPeerDataOverload(long peerDataOverload) {
            this.peerDataOverload = peerDataOverload;
        }

        public long getFlowOverload() {
            return flowOverload;
        }

        public void setFlowOverload(long flowOverload) {
            this.flowOverload = flowOverload;
        }

        @Override
        public String toString() {
            return "DiskType{" +
                    "diskType='" + diskType + '\'' +
                    ", peerDataOverload=" + peerDataOverload +
                    ", flowOverload=" + flowOverload +
                    '}';
        }
    }

    public enum DiskType{

        DEFAULT("default"),
        RAID0("raid0"),
        RAID10("raid10");
        DiskType(String desc){
            this.desc = desc;
        }

        private final String desc;

        public String getDesc() {
            return desc;
        }

    }

}
