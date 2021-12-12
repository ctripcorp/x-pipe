package com.ctrip.xpipe.redis.meta.server.rest;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;

import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Aug 4, 2016
 */
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

	public ClusterDebugInfo(int currentServerId, String currentDc, String zkAddress, String zkNameSpace, boolean isLeader, 
			ClusterServerInfo currentClusterServerInfo, Set<Integer> inchargeSlots, Set<Long> inchargeClusters,
			Map<Integer, ClusterServerInfo> clusterServerInfos, 
			Map<Integer, SlotInfo> allSlotInfo){
		this.currentServerId = currentServerId;
		this.currentDc = currentDc;
		this.zkAddress = zkAddress;
		this.zkNameSpace = zkNameSpace;
		this.isLeader = isLeader;
		this.currentClusterServerInfo = currentClusterServerInfo;
		this.inchargeSlots = inchargeSlots;
		this.inchargeClusters = inchargeClusters;
		this.clusterServerInfos = clusterServerInfos;
		this.allSlotInfo = allSlotInfo;
	}
	
	public int getCurrentServerId() {
		return currentServerId;
	}
	
	public String getCurrentDc() {
		return currentDc;
	}
	
	public boolean isLeader() {
		return isLeader;
	}

	public String getZkAddress() {
		return zkAddress;
	}

	public String getZkNameSpace() {
		return zkNameSpace;
	}
	
	public ClusterServerInfo getCurrentClusterServerInfo() {
		return currentClusterServerInfo;
	}
	
	public int getInchargeSlotsSize() {
		if(inchargeSlots != null){
			return inchargeSlots.size();
		}
		return -1;
	}

	public Set<Integer> getInchargeSlots() {
		return inchargeSlots;
	}
	public Set<Long> getInchargeClusters() {
		return inchargeClusters;
	}
	
	public Map<Integer, ClusterServerInfo> getClusterServerInfos() {
		return clusterServerInfos;
	}
	
	public Map<Integer, SlotInfo> getAllSlotInfo() {
		return allSlotInfo;
	}
	
}
