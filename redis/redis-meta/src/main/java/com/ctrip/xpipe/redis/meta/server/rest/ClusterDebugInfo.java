package com.ctrip.xpipe.redis.meta.server.rest;

import java.util.Map;
import java.util.Set;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;

/**
 * @author wenchao.meng
 *
 * Aug 4, 2016
 */
public class ClusterDebugInfo {
	
	private int currentServerId;
	private boolean isLeader;
	private ClusterServerInfo currentClusterServerInfo;
	private Set<Integer> inchargeSlots;
	private Set<String> inchargeClusters;
	private Map<Integer, ClusterServerInfo> clusterServerInfos;
	private Map<Integer, SlotInfo> allSlotInfo;

	public ClusterDebugInfo(int currentServerId, boolean isLeader, ClusterServerInfo currentClusterServerInfo, Set<Integer> inchargeSlots, 
			Map<Integer, ClusterServerInfo> clusterServerInfos, 
			Map<Integer, SlotInfo> allSlotInfo){
		this.currentServerId = currentServerId;
		this.isLeader = isLeader;
		this.currentClusterServerInfo = currentClusterServerInfo;
		this.inchargeSlots = inchargeSlots;
		this.clusterServerInfos = clusterServerInfos;
		this.allSlotInfo = allSlotInfo;
	}
	
	public int getCurrentServerId() {
		return currentServerId;
	}
	public ClusterServerInfo getCurrentClusterServerInfo() {
		return currentClusterServerInfo;
	}
	public Set<Integer> getInchargeSlots() {
		return inchargeSlots;
	}
	public Set<String> getInchargeClusters() {
		return inchargeClusters;
	}
	
	public boolean isLeader() {
		return isLeader;
	}
	
	public Map<Integer, ClusterServerInfo> getClusterServerInfos() {
		return clusterServerInfos;
	}
	
	public Map<Integer, SlotInfo> getAllSlotInfo() {
		return allSlotInfo;
	}
	
}
