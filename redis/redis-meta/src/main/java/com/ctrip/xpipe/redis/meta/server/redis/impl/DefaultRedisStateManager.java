package com.ctrip.xpipe.redis.meta.server.redis.impl;

import java.util.concurrent.*;

import javax.annotation.Resource;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.redis.RedisStateManager;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;

/**
 * @author wenchao.meng
 *
 * Dec 26, 2016
 */
public class DefaultRedisStateManager extends AbstractLifecycle implements RedisStateManager, TopElement{

	private int redisStateManagerIntervalSeconds = Integer.parseInt(System.getProperty("REDIS_STATE_MANAGR_INTERVAL_SECONDS", "5")); 
	
	@Autowired
	private CurrentMetaManager currentMetaManager;
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
	private Executor executors;

	private ScheduledFuture<?> future;

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();

	}
	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		future = scheduled.scheduleWithFixedDelay(new RedisesStateChangeTask(), redisStateManagerIntervalSeconds, redisStateManagerIntervalSeconds, TimeUnit.SECONDS);
	}

	@Override
	protected void doStop() throws Exception {

		if(future != null){
			future.cancel(true);
		}
		super.doStop();
	}
	
	@Override
	protected void doDispose() throws Exception {
		super.doDispose();
	}

	class RedisesStateChangeTask extends AbstractExceptionLogTask{

		protected void doRun() throws Exception {
			
			for(String clusterId : currentMetaManager.allClusters()){
				
				if(dcMetaCache.isCurrentDcPrimary(clusterId)){
					executors.execute(new PrimaryDcClusterRedisStateAjust());
				}else{
					executors.execute(new BackupDcClusterRedisStateAjust(clusterId, currentMetaManager, keyedObjectPool, scheduled, executors));
				}
			}
		}
	}

}
