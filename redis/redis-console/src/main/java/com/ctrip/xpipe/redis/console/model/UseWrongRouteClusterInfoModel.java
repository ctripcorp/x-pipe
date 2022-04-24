package com.ctrip.xpipe.redis.console.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.*;

@JsonPropertyOrder({"useWrongRouteClusterNum", "useWrongRouteClusterNumWithDirection", "useWrongRouteClusterDetails"})
public class UseWrongRouteClusterInfoModel {

    private int useWrongRouteClusterNum;

    private Map<String, Integer> useWrongRouteClusterNumWithDirection;

    private Map<String, List<UseWrongRouteClusterDetail>> useWrongRouteClusterDetails;

    public UseWrongRouteClusterInfoModel() {
        this.useWrongRouteClusterDetails = new HashMap<>();
        this.useWrongRouteClusterNumWithDirection = new HashMap<>();
    }

    public void addUsedWrongRouteCluster(String clusterName, String srcDcName, String dstDcName, Set<Integer> usedRouteId, Integer chooseRouteId) {
        if(!useWrongRouteClusterDetails.containsKey(clusterName)) {
            useWrongRouteClusterNum++;
            this.useWrongRouteClusterDetails.put(clusterName, new ArrayList<>());
        }
        this.useWrongRouteClusterDetails.get(clusterName).add(new UseWrongRouteClusterDetail(clusterName, srcDcName, dstDcName, usedRouteId, chooseRouteId))
;
        String direction = String.format("%s------>%s", srcDcName, dstDcName);
        if(!useWrongRouteClusterNumWithDirection.containsKey(direction)) {
            this.useWrongRouteClusterNumWithDirection.put(direction, 1);
        } else {
            this.useWrongRouteClusterNumWithDirection.put(direction, this.useWrongRouteClusterNumWithDirection.get(direction) + 1);
        }
    }

    public int getUseWrongRouteClusterNum() {
        return useWrongRouteClusterNum;
    }

    public UseWrongRouteClusterInfoModel setUseWrongRouteClusterNum(int useWrongRouteClusterNum) {
        this.useWrongRouteClusterNum = useWrongRouteClusterNum;
        return this;
    }

    public Map<String, Integer> getUseWrongRouteClusterNumWithDirection() {
        return useWrongRouteClusterNumWithDirection;
    }

    public UseWrongRouteClusterInfoModel setUseWrongRouteClusterNumWithDirection(Map<String, Integer> useWrongRouteClusterNumWithDirection) {
        this.useWrongRouteClusterNumWithDirection = useWrongRouteClusterNumWithDirection;
        return this;
    }

    public Map<String, List<UseWrongRouteClusterDetail>> getUseWrongRouteClusterDetails() {
        return useWrongRouteClusterDetails;
    }

    public UseWrongRouteClusterInfoModel setUseWrongRouteClusterDetails(Map<String, List<UseWrongRouteClusterDetail>> useWrongRouteClusterDetails) {
        this.useWrongRouteClusterDetails = useWrongRouteClusterDetails;
        return this;
    }

    public static class UseWrongRouteClusterDetail {
        private String clusterName;

        private String srcDcName;

        private String dstDcName;

        private Set<Integer> usedRouteId;

        private int chooseRouteId;

        public UseWrongRouteClusterDetail() {
        }

        public UseWrongRouteClusterDetail(String clusterName, String srcDcName, String dstDcName, Set<Integer> usedRouteId, int chooseRouteId) {
            this.clusterName = clusterName;
            this.srcDcName = srcDcName;
            this.dstDcName = dstDcName;
            this.usedRouteId = usedRouteId;
            this.chooseRouteId = chooseRouteId;
        }

        public String getClusterName() {
            return clusterName;
        }

        public UseWrongRouteClusterDetail setClusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public String getSrcDcName() {
            return srcDcName;
        }

        public UseWrongRouteClusterDetail setSrcDcName(String srcDcName) {
            this.srcDcName = srcDcName;
            return this;
        }

        public String getDstDcName() {
            return dstDcName;
        }

        public UseWrongRouteClusterDetail setDstDcName(String dstDcName) {
            this.dstDcName = dstDcName;
            return this;
        }

        public Set<Integer> getUsedRouteId() {
            return usedRouteId;
        }

        public UseWrongRouteClusterDetail setUsedRouteId(Set<Integer> usedRouteId) {
            this.usedRouteId = usedRouteId;
            return this;
        }

        public int getChooseRouteId() {
            return chooseRouteId;
        }

        public UseWrongRouteClusterDetail setChooseRouteId(int chooseRouteId) {
            this.chooseRouteId = chooseRouteId;
            return this;
        }

        @Override
        public String toString() {
            return "WrongRouteUsedClusterInfo{" +
                    "clusterName='" + clusterName + '\'' +
                    ", srcDcName='" + srcDcName + '\'' +
                    ", dstDcName='" + dstDcName + '\'' +
                    ", usedRouteId=" + usedRouteId +
                    ", chooseRouteId=" + chooseRouteId +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UseWrongRouteClusterDetail that = (UseWrongRouteClusterDetail) o;
            return usedRouteId == that.usedRouteId && chooseRouteId == that.chooseRouteId && Objects.equals(clusterName, that.clusterName)
                    && Objects.equals(srcDcName, that.srcDcName) && Objects.equals(dstDcName, that.dstDcName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterName, srcDcName, dstDcName, usedRouteId, chooseRouteId);
        }
    }
}
