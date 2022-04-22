package com.ctrip.xpipe.redis.console.model;

import java.util.*;

public class WrongRouteUsedInfoModel {

    private int wrongRouteUsedClusterNum;

    private Map<String, Integer> wrongRouteUsedDirectionInfos;

    private Map<String, List<WrongRouteUsedClusterInfo> > wrongRouteUsedClusterInfos;

    public WrongRouteUsedInfoModel() {
        this.wrongRouteUsedClusterInfos = new HashMap<>();
        this.wrongRouteUsedDirectionInfos = new HashMap<>();
    }

    public void addWrongRouteUsedCluster(String clusterName, String srcDcName, String dstDcName, Set<Integer> usedRouteId, int chooseRouteId) {
        if(!wrongRouteUsedClusterInfos.containsKey(clusterName)) {
            wrongRouteUsedClusterNum++;
            this.wrongRouteUsedClusterInfos.put(clusterName, new ArrayList<>());
        }
        this.wrongRouteUsedClusterInfos.get(clusterName).add(new WrongRouteUsedClusterInfo(clusterName, srcDcName, dstDcName, usedRouteId, chooseRouteId))
;
        String direction = String.format("%s------>%s", srcDcName, dstDcName);
        if(!wrongRouteUsedDirectionInfos.containsKey(direction)) {
            this.wrongRouteUsedDirectionInfos.put(direction, 1);
        } else {
            this.wrongRouteUsedDirectionInfos.put(direction, this.wrongRouteUsedDirectionInfos.get(direction) + 1);
        }
    }

    public int getWrongRouteUsedClusterNum() {
        return wrongRouteUsedClusterNum;
    }

    public WrongRouteUsedInfoModel setWrongRouteUsedClusterNum(int wrongRouteUsedClusterNum) {
        this.wrongRouteUsedClusterNum = wrongRouteUsedClusterNum;
        return this;
    }

    public Map<String, Integer> getWrongRouteUsedDirection() {
        return wrongRouteUsedDirectionInfos;
    }

    public WrongRouteUsedInfoModel setWrongRouteUsedDirection(Map<String, Integer> wrongRouteUsedDirectionInfos) {
        this.wrongRouteUsedDirectionInfos = wrongRouteUsedDirectionInfos;
        return this;
    }

    public Map<String, List<WrongRouteUsedClusterInfo>> getWrongRouteUsedClusterInfos() {
        return wrongRouteUsedClusterInfos;
    }

    public WrongRouteUsedInfoModel setWrongRouteUsedClusterInfos(Map<String, List<WrongRouteUsedClusterInfo>> wrongRouteUsedClusterInfos) {
        this.wrongRouteUsedClusterInfos = wrongRouteUsedClusterInfos;
        return this;
    }

    public static class WrongRouteUsedClusterInfo {
        private String clusterName;

        private String srcDcName;

        private String dstDcName;

        private Set<Integer> usedRouteId;

        private int chooseRouteId;

        public WrongRouteUsedClusterInfo() {
        }

        public WrongRouteUsedClusterInfo(String clusterName, String srcDcName, String dstDcName, Set<Integer> usedRouteId, int chooseRouteId) {
            this.clusterName = clusterName;
            this.srcDcName = srcDcName;
            this.dstDcName = dstDcName;
            this.usedRouteId = usedRouteId;
            this.chooseRouteId = chooseRouteId;
        }

        public String getClusterName() {
            return clusterName;
        }

        public WrongRouteUsedClusterInfo setClusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public String getSrcDcName() {
            return srcDcName;
        }

        public WrongRouteUsedClusterInfo setSrcDcName(String srcDcName) {
            this.srcDcName = srcDcName;
            return this;
        }

        public String getDstDcName() {
            return dstDcName;
        }

        public WrongRouteUsedClusterInfo setDstDcName(String dstDcName) {
            this.dstDcName = dstDcName;
            return this;
        }

        public Set<Integer> getUsedRouteId() {
            return usedRouteId;
        }

        public WrongRouteUsedClusterInfo setUsedRouteId(Set<Integer> usedRouteId) {
            this.usedRouteId = usedRouteId;
            return this;
        }

        public int getChooseRouteId() {
            return chooseRouteId;
        }

        public WrongRouteUsedClusterInfo setChooseRouteId(int chooseRouteId) {
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
            WrongRouteUsedClusterInfo that = (WrongRouteUsedClusterInfo) o;
            return usedRouteId == that.usedRouteId && chooseRouteId == that.chooseRouteId && Objects.equals(clusterName, that.clusterName)
                    && Objects.equals(srcDcName, that.srcDcName) && Objects.equals(dstDcName, that.dstDcName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterName, srcDcName, dstDcName, usedRouteId, chooseRouteId);
        }
    }
}
