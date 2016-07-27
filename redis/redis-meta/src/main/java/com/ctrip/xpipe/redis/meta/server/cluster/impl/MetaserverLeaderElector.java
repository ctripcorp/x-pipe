package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
@Component
public class MetaserverLeaderElector extends AbstractLifecycleObservable implements LeaderLatchListener, ApplicationContextAware, TopElement{


	@Autowired
	private ZkClient zkClient;
	
	@Autowired
	private MetaServerConfig config;
	
	
	private ApplicationContext applicationContext;
	
	private LeaderLatch leaderLatch;
	
	private ExecutorService executors  = Executors.newCachedThreadPool(XpipeThreadFactory.create("META-SREVER-LEADER-ELECTOR"));
	
	
	@Override
	protected void doStart() throws Exception {
		
		leaderLatch = new LeaderLatch(zkClient.get(), MetaZkConfig.getMetaServerLeaderElectPath(), String.valueOf(config.getMetaServerId()));
		leaderLatch.addListener(this, executors);
		leaderLatch.start();
		
	}

	@Override
	public void isLeader() {
		logger.info("[isLeader]");
		Map<String, LeaderAware> leaderawares = applicationContext.getBeansOfType(LeaderAware.class);
		for(Entry<String, LeaderAware> entry : leaderawares.entrySet()){
			logger.info("[isLeader][notify]{}", entry.getKey());
			entry.getValue().isleader();
		}
	}

	@Override
	public void notLeader() {
		logger.info("[notLeader]");
		Map<String, LeaderAware> leaderawares = applicationContext.getBeansOfType(LeaderAware.class);
		for(Entry<String, LeaderAware> entry : leaderawares.entrySet()){
			logger.info("[notLeader][notify]{}", entry.getKey());
			entry.getValue().notLeader();
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}
}
