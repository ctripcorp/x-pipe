package com.ctrip.xpipe.redis.meta.server.rest;

import java.util.Set;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;

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

	public ClusterDebugInfo(int currentServerId, boolean isLeader, ClusterServerInfo currentClusterServerInfo, Set<Integer> inchargeSlots){
		this.currentServerId = currentServerId;
		this.isLeader = isLeader;
		this.currentClusterServerInfo = currentClusterServerInfo;
		this.inchargeSlots = inchargeSlots;
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
	
}
