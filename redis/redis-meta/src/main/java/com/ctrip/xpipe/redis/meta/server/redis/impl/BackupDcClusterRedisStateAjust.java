package com.ctrip.xpipe.redis.meta.server.redis.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.job.BackupDcClusterShardAdjustJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 26, 2016
 */
public class BackupDcClusterRedisStateAjust extends AbstractClusterRedisStateAjustTask{
	
	private String clusterId;
	
	private CurrentMetaManager currentMetaManager;

	private DcMetaCache dcMetaCache;
	
	private XpipeNettyClientKeyedObjectPool pool;
	
	private ScheduledExecutorService scheduled;

	private Executor executors;

	private KeyedOneThreadMutexableTaskExecutor<Pair<String, String> > clusterShardExecutors;
	
	public BackupDcClusterRedisStateAjust(String clusterId, DcMetaCache dcMetaCache,
										  CurrentMetaManager currentMetaManager, XpipeNettyClientKeyedObjectPool pool,
										  ScheduledExecutorService scheduled, Executor executors,
										  KeyedOneThreadMutexableTaskExecutor<Pair<String, String> > clusterShardExecutors) {
		this.clusterId = clusterId;
		this.currentMetaManager = currentMetaManager;
		this.pool = pool;
		this.scheduled = scheduled;
		this.executors = executors;
		this.dcMetaCache = dcMetaCache;
		this.clusterShardExecutors = clusterShardExecutors;
	}
	

	@Override
	protected void doRun() throws Exception {

		ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterId);
		if(clusterMeta == null){
			logger.warn("[doRun][cluster null]{}", clusterId);
			return;
		}
		
		for(ShardMeta shardMeta : clusterMeta.getShards().values()) {
			try {
				Command<Void> adjustJob = new BackupDcClusterShardAdjustJob(clusterId, shardMeta.getId(), dcMetaCache,
						currentMetaManager, executors, scheduled, pool);
				clusterShardExecutors.execute(new Pair<>(clusterId, shardMeta.getId()), adjustJob);
			} catch (Exception e) {
				logger.info("[doRun] {}, {} adjust fail {}", clusterId, shardMeta.getId(), e.getMessage());
			}
		}
	}

}
