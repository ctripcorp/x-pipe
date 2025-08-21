package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ReshardingTask;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ServerBalanceResharding;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ServerDeadResharding;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.zk.ZkClient;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * if server restart, we dont want to do sharding twice
 * @author wenchao.meng
 *
 * Jul 27, 2016
 */
@Component
public class ArrangeTaskTrigger extends AbstractLifecycle implements TopElement{
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	private int waitForRestartTimeMills = 5000;
	
	@Autowired
	private SlotManager slotManager;
	
	@Autowired
	private ZkClient zkClient;

	@Autowired
	private ClusterServers<?> clusterServers;

	@Autowired
	private ArrangeTaskExecutor arrangeTaskExecutor;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;
	
	private Map<ClusterServer, DeadServer> serverActions = new ConcurrentHashMap<>();

	public void initSharding(ReshardingTask task){
		
		arrangeTaskExecutor.offer(task);
	}
	
	public void rebalance(){
		arrangeTaskExecutor.offer(new ServerBalanceResharding(slotManager, clusterServers, zkClient));
	}
	
	public void serverDead(final ClusterServer clusterServer){
		
		logger.info("[serverDead]{}", clusterServer);
		
		MapUtils.getOrCreate(serverActions, clusterServer, new ObjectFactory<DeadServer>() {

			@Override
			public DeadServer create() {
				return new DeadServer(clusterServer);
			}
		});
	}

	private void remove(ClusterServer clusterServer) {
		serverActions.remove(clusterServer);
	}

	public void serverAlive(ClusterServer clusterServer){

		logger.info("[serverAlive]{}", clusterServer);
		DeadServer deadServer = serverActions.get(clusterServer);
		if(deadServer == null){
			arrangeTaskExecutor.offer(new ServerBalanceResharding(slotManager, clusterServers, zkClient));
		}else{
			logger.info("[serverAlive][dead server alive]{}", clusterServer);
			deadServer.serverAlive();
		}
	}
	
	
	public class DeadServer implements Runnable{
		
		private ClusterServer clusterServer;
		private ScheduledFuture<?> future;
		public DeadServer(ClusterServer clusterServer){
			this.clusterServer = clusterServer;
			future = scheduled.schedule(this, waitForRestartTimeMills, TimeUnit.MILLISECONDS);
		}
		
		public void serverAlive(){
			logger.info("[serverAlive][cancel]");
			future.cancel(true);
			remove(clusterServer);
		}
		

		@Override
		public void run() {
			remove(clusterServer);
			arrangeTaskExecutor.offer(new ServerDeadResharding(slotManager, clusterServer, clusterServers, zkClient));
		}
	} 

	public void setWaitForRestartTimeMills(int waitForRestartTimeMills) {
		this.waitForRestartTimeMills = waitForRestartTimeMills;
	}
	
	public void setArrangeTaskExecutor(ArrangeTaskExecutor arrangeTaskExecutor) {
		this.arrangeTaskExecutor = arrangeTaskExecutor;
	}

	public void setScheduled(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}
}
