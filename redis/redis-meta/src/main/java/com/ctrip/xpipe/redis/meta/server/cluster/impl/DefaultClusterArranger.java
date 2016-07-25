package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.HashSet;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterArranger;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
@Component
public class DefaultClusterArranger extends AbstractLifecycle implements ClusterArranger, TopElement, LeaderAware, Observer{
	
	@Autowired
	private ZkClient zkClient;
	
	private int waitDeadRestart = 30000;
	
	@Autowired
	private ClusterServers clusterServers;
	
	@Autowired
	private SlotManager slotManager;
	
	private volatile boolean leader = false;

	
	@Override
	public void isleader() {
		
		logger.info("[isLeader]");
		leader = true;
		
		try {
			checkAllSlots();
		} catch (Exception e) {
			logger.error("[isLeader]", e);
		}
		
		clusterServers.addObserver(this);
	}

	private void checkAllSlots() throws Exception {
		
		slotManager.refresh();
		clusterServers.refresh();
		
		Set<Integer> notExist =  new HashSet<>();
		Set<Integer> deadServer =  new HashSet<>();
				
		Set<Integer> allSlots =  slotManager.allSlots();
		for(int i=0;i<SlotManager.TOTAL_SLOTS;i++){
			if(!allSlots.contains(i)){
				notExist.add(i);
			}
		}
		
		if(notExist.size() > 0){
			logger.info("[checkAllSlots][not exist]{}", notExist);
		}
		
		Set<Integer> allSlotServers = slotManager.allServers();
		for(int slotServer : allSlotServers){
			if(!clusterServers.exist(slotServer)){
				deadServer.add(slotServer);
			}
		}

		if(deadServer.size() > 0){
			logger.info("[checkAllSlots][dead servers]{}", deadServer);
		}
		
		
		
		
	}

	@Override
	public void notLeader() {
		logger.info("[notLeader]");
		leader = false;
		clusterServers.removeObserver(this);
	}

	@Override
	public int getOrder() {
		return 0;
	}

	@Override
	public void onServerAdded(int clusterServer) {
		if(!leader){
			return;
		}
		
	}

	@Override
	public void onServerRemoved(int clusterServer) {
		if(!leader){
			return;
		}
		
		
		
	}

	@Override
	public void onServerChanged(ClusterServer oldClusterServer, ClusterServer newClusterServer) {
		if(!leader){
			return;
		}
		
		logger.info("[onNodeChanged][nothing to do]{}->{}", oldClusterServer, newClusterServer);
	}

	@Override
	public void moveSlot(int slotId, int fromServer, int toServer) {
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof NodeAdded<?>){
			NodeAdded<ClusterServer> added = (NodeAdded<ClusterServer>) args;
			onServerAdded(added.getNode().getServerId());
			return;
		}

		if(args instanceof NodeDeleted<?>){
			NodeDeleted<ClusterServer> deleted = (NodeDeleted<ClusterServer>) args;
			onServerRemoved(deleted.getNode().getServerId());
			return;
		}

		if(args instanceof NodeModified<?>){
			NodeModified<ClusterServer> modified = (NodeModified<ClusterServer>) args;
			onServerChanged(modified.getOldNode(), modified.getNewNode());
			return;
		}
		
		throw new IllegalArgumentException("unknown:" + args + ", from:" + observable);
	}

}
