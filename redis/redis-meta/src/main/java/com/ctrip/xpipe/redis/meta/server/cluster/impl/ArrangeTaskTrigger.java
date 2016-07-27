package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ReshardingTask;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ServerBalanceResharding;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ServerDeadResharding;

/**
 * if server restart, we dont want to do sharding twice
 * @author wenchao.meng
 *
 * Jul 27, 2016
 */
@Component
public class ArrangeTaskTrigger {
	
	private Map<ClusterServer, > 

	@Autowired
	private ArrangeTaskExecutor arrangeTaskExecutor;
	
	public void initSharding(ReshardingTask task){
		arrangeTaskExecutor.offer(task);
	}
	
	public void serverDead(ClusterServer clusterServer){

		arrangeTaskExecutor.offer(new ServerBalanceResharding(slotManager, clusterServers, zkClient));
		arrangeTaskExecutor.offer(new ServerDeadResharding(slotManager, clusterServer, clusterServers, zkClient));
	}
	
	public void serverAdded(ClusterServer clusterServer){
		
	}
	

}
