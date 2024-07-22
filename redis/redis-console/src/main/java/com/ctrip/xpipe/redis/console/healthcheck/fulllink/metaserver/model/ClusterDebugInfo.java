package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.model;

import java.util.Map;
import java.util.Set;

public class ClusterDebugInfo {

    private int currentServerId;
    private String currentDc;
    private String zkAddress;
    private boolean isLeader;
    private ClusterServerInfo currentClusterServerInfo;
    private Set<Integer> inchargeSlots;
    private Set<Long> inchargeClusters;
    private Map<Integer, ClusterServerInfo> clusterServerInfos;
    private Map<Integer, SlotInfo> allSlotInfo;
    private String zkNameSpace;

    public int getCurrentServerId() {
        return currentServerId;
    }

    public void setCurrentServerId(int currentServerId) {
        this.currentServerId = currentServerId;
    }

    public String getCurrentDc() {
        return currentDc;
    }

    public void setCurrentDc(String currentDc) {
        this.currentDc = currentDc;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    public ClusterServerInfo getCurrentClusterServerInfo() {
        return currentClusterServerInfo;
    }

    public void setCurrentClusterServerInfo(ClusterServerInfo currentClusterServerInfo) {
        this.currentClusterServerInfo = currentClusterServerInfo;
    }

    public Set<Integer> getInchargeSlots() {
        return inchargeSlots;
    }

    public void setInchargeSlots(Set<Integer> inchargeSlots) {
        this.inchargeSlots = inchargeSlots;
    }

    public Set<Long> getInchargeClusters() {
        return inchargeClusters;
    }

    public void setInchargeClusters(Set<Long> inchargeClusters) {
        this.inchargeClusters = inchargeClusters;
    }

    public Map<Integer, ClusterServerInfo> getClusterServerInfos() {
        return clusterServerInfos;
    }

    public void setClusterServerInfos(Map<Integer, ClusterServerInfo> clusterServerInfos) {
        this.clusterServerInfos = clusterServerInfos;
    }

    public Map<Integer, SlotInfo> getAllSlotInfo() {
        return allSlotInfo;
    }

    public void setAllSlotInfo(Map<Integer, SlotInfo> allSlotInfo) {
        this.allSlotInfo = allSlotInfo;
    }

    public String getZkNameSpace() {
        return zkNameSpace;
    }

    public void setZkNameSpace(String zkNameSpace) {
        this.zkNameSpace = zkNameSpace;
    }
}
