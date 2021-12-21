package com.ctrip.xpipe.redis.meta.server.redis.impl;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.redis.ClusterRedisStateAjustTask;
import com.ctrip.xpipe.redis.meta.server.redis.RedisStateManager;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

	@Resource(name = AbstractSpringConfigContext.CLUSTER_SHARD_ADJUST_EXECUTOR)
	private KeyedOneThreadMutexableTaskExecutor<Pair<Long, Long> > clusterShardExecutors;

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
			
			for(Long clusterDbId : currentMetaManager.allClusters()){

				ClusterRedisStateAjustTask adjustTask = buildAdjustTaskForCluster(clusterDbId);
				if (null != adjustTask) {
					executors.execute(adjustTask);
				}

			}
		}

		private ClusterRedisStateAjustTask buildAdjustTaskForCluster(Long clusterDbId) {
			ClusterType type;

			try {
				type = dcMetaCache.getClusterType(clusterDbId);
			} catch (Exception e) {
				logger.info("[buildAdjustTaskForCluster] get type for cluster_{} fail", clusterDbId, e);
				return null;
			}

			switch (type) {
				case ONE_WAY:
					if (dcMetaCache.isCurrentDcPrimary(clusterDbId)) {
						return new PrimaryDcClusterRedisStateAjust();
					} else {
						return new BackupDcClusterRedisStateAjust(clusterDbId, dcMetaCache, currentMetaManager,
								keyedObjectPool, scheduled, executors, clusterShardExecutors);
					}
				case BI_DIRECTION:
				default:
					return null;
			}
		}
	}

}
