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
	
	private Long clusterDbId;
	
	private CurrentMetaManager currentMetaManager;

	private DcMetaCache dcMetaCache;
	
	private XpipeNettyClientKeyedObjectPool pool;
	
	private ScheduledExecutorService scheduled;

	private Executor executors;

	private KeyedOneThreadMutexableTaskExecutor<Pair<Long, Long> > clusterShardExecutors;
	
	public BackupDcClusterRedisStateAjust(Long clusterDbId, DcMetaCache dcMetaCache,
										  CurrentMetaManager currentMetaManager, XpipeNettyClientKeyedObjectPool pool,
										  ScheduledExecutorService scheduled, Executor executors,
										  KeyedOneThreadMutexableTaskExecutor<Pair<Long, Long> > clusterShardExecutors) {
		this.clusterDbId = clusterDbId;
		this.currentMetaManager = currentMetaManager;
		this.pool = pool;
		this.scheduled = scheduled;
		this.executors = executors;
		this.dcMetaCache = dcMetaCache;
		this.clusterShardExecutors = clusterShardExecutors;
	}
	

	@Override
	protected void doRun() throws Exception {

		ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterDbId);
		if(clusterMeta == null){
			logger.warn("[doRun][cluster null]{}", clusterDbId);
			return;
		}
		
		for(ShardMeta shardMeta : clusterMeta.getShards().values()) {
			try {
				Command<Void> adjustJob = new BackupDcClusterShardAdjustJob(clusterDbId, shardMeta.getDbId(), dcMetaCache,
						currentMetaManager, executors, scheduled, pool);
				clusterShardExecutors.execute(new Pair<>(clusterDbId, shardMeta.getDbId()), adjustJob);
			} catch (Exception e) {
				logger.info("[doRun] {}, {} adjust fail {}", clusterDbId, shardMeta.getDbId(), e.getMessage());
			}
		}
	}

}
