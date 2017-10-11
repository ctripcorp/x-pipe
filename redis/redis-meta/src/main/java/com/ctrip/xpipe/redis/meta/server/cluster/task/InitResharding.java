package com.ctrip.xpipe.redis.meta.server.cluster.task;


import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.zk.ZkClient;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class InitResharding extends AbstractDirectMoveSharding{
	
	private Set<Integer> emptySlots;
	
	public InitResharding(SlotManager slotManager, Set<Integer> emptySlots, ClusterServers<?> servers, ZkClient zkClient) {
		super(slotManager, servers, zkClient);
		this.emptySlots = emptySlots;
	}

	@Override
	protected ClusterServer getDeadServer() {
		return null;
	}

	@Override
	protected List<Integer> getSlotsToArrange() {
		return new LinkedList<>(emptySlots);
	}

	@Override
	protected void doReset(){
		
	}
}
