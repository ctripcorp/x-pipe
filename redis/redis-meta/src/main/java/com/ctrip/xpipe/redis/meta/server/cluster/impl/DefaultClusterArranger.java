package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.redis.meta.server.cluster.*;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ContinueResharding;
import com.ctrip.xpipe.redis.meta.server.cluster.task.InitResharding;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.zk.ZkClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
@Component
public class DefaultClusterArranger extends AbstractLifecycle implements ClusterArranger, TopElement, MetaServerLeaderAware, Observer{
	
	@Autowired
	private ClusterServers<?> clusterServers;
	
	@Autowired
	private ZkClient zkClient;
	
	@Autowired
	private ArrangeTaskTrigger arrangeTaskTrigger;
	
	@Autowired
	private RemoteClusterServerFactory<?> remoteClusterServerFactory;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Autowired
	private SlotManager slotManager;
	
	@Autowired
	private MetaServerConfig config;
	
	private AtomicBoolean leader = new AtomicBoolean(false);
	private ScheduledFuture<?> future;

	
	@Override
	public void isleader() {
		
		logger.info("[isLeader]");
		leader.set(true);
		
		try {
			initCheck();
		} catch (Exception e) {
			logger.error("[isLeader]", e);
		}
		
		future = scheduled.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try {
					periodCheck();
				} catch (Throwable th) {
					logger.error("[run]", th);
				}
			}
		}, 0, config.getLeaderCheckMilli(), TimeUnit.MILLISECONDS);
		
		clusterServers.addObserver(this);
	}

	
	@Override
	protected void doDispose() throws Exception {
		
		super.doDispose();
	}
	
	private void refresh() throws ClusterException {
		slotManager.refresh();
		clusterServers.refresh();
		
	}

	protected void periodCheck() {
		
		checkDeadServer();
		arrangeTaskTrigger.rebalance();
	}

	private void initCheck() throws ClusterException {
		refresh();
		
		checkNotExist();
		checkMovingTasks();
		checkDeadServer();
	}

	private void checkDeadServer() {
		Set<Integer> deadServer = new HashSet<>();
		
		
		Set<Integer> allSlotServers = slotManager.allServers();
		for(int slotServer : allSlotServers){
			if(!clusterServers.exist(slotServer)){
				deadServer.add(slotServer);
			}
		}

		if(deadServer.size() > 0){
			logger.info("[checkAllSlots][dead servers]{}", deadServer);
			for(Integer deadServerId : deadServer){
				onServerRemoved(remoteClusterServerFactory.createClusterServer(deadServerId, null));
			}
		}
	}

	private void checkMovingTasks() {
		//moving slots
		Map<Integer, SlotInfo> movingSlots = slotManager.allMoveingSlots();
		if(movingSlots.size() > 0){
			arrangeTaskTrigger.initSharding(new ContinueResharding(slotManager, movingSlots, clusterServers, remoteClusterServerFactory, zkClient));
		}
	}

	private void checkNotExist() {
		
		Set<Integer> notExist =  new HashSet<>();
		Set<Integer> allSlots =  slotManager.allSlots();
		for(int i=0;i<SlotManager.TOTAL_SLOTS;i++){
			if(!allSlots.contains(i)){
				notExist.add(i);
			}
		}
		
		if(notExist.size() > 0){
			logger.info("[checkAllSlots][not exist]{}", notExist);
			arrangeTaskTrigger.initSharding(new InitResharding(slotManager, notExist, clusterServers, zkClient));
		}
		
	}

	@Override
	public void notLeader() {
		logger.info("[notCrossDcLeader]");
		if(leader.compareAndSet(true, false)){
			clusterServers.removeObserver(this);
			if(future != null){
				future.cancel(true);
			}
		}
	}

	@Override
	public int getOrder() {
		return 0;
	}

	@Override
	public void onServerAdded(ClusterServer clusterServer) {
		if(!leader.get()){
			return;
		}
		logger.info("[onServerAdded]{}", clusterServer);
		arrangeTaskTrigger.serverAlive(clusterServer);
		
	}

	@Override
	public void onServerRemoved(ClusterServer clusterServer) {
		if(!leader.get()){
			return;
		}
		logger.info("[onServerRemoved]{}", clusterServer);
		arrangeTaskTrigger.serverDead(clusterServer);
	}

	@Override
	public void onServerChanged(ClusterServer oldClusterServer, ClusterServer newClusterServer) {
		if(!leader.get()){
			return;
		}
		logger.info("[onNodeChanged][nothing to do]{}->{}", oldClusterServer, newClusterServer);
	}


	@SuppressWarnings("unchecked")
	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof NodeAdded<?>){
			NodeAdded<ClusterServer> added = (NodeAdded<ClusterServer>) args;
			onServerAdded(added.getNode());
			return;
		}

		if(args instanceof NodeDeleted<?>){
			NodeDeleted<ClusterServer> deleted = (NodeDeleted<ClusterServer>) args;
			onServerRemoved(deleted.getNode());
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
