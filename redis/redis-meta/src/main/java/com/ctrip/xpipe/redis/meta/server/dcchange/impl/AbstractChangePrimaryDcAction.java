package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public abstract class AbstractChangePrimaryDcAction implements ChangePrimaryDcAction{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public static final int DEFAULT_CHANGE_PRIMARY_WAIT_TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("DEFAULT_CHANGE_PRIMARY_WAIT_TIMEOUT_SECONDS", "2"));
	
	protected int waitTimeoutSeconds = DEFAULT_CHANGE_PRIMARY_WAIT_TIMEOUT_SECONDS;

	protected Long cluster;

	protected Long shard;
	
	protected ExecutionLog executionLog;
	
	protected DcMetaCache   dcMetaCache;
	
	protected CurrentMetaManager currentMetaManager;
	
	protected SentinelManager sentinelManager;
	
	protected XpipeNettyClientKeyedObjectPool keyedObjectPool;
	
	protected ScheduledExecutorService scheduled;

	protected Executor executors;

	public AbstractChangePrimaryDcAction(Long clusterDbId, Long shardDbId,
										 DcMetaCache dcMetaCache,
										 CurrentMetaManager currentMetaManager,
										 SentinelManager sentinelManager,
										 ExecutionLog executionLog,
										 XpipeNettyClientKeyedObjectPool keyedObjectPool,
										 ScheduledExecutorService scheduled,
										 Executor executors) {
		this.cluster = clusterDbId;
		this.shard = shardDbId;
		this.dcMetaCache = dcMetaCache;
		this.currentMetaManager = currentMetaManager;
		this.sentinelManager = sentinelManager;
		this.executionLog = executionLog;
		this.keyedObjectPool = keyedObjectPool;
		this.scheduled = scheduled;
		this.executors = executors;
	}

	@Override
	public PrimaryDcChangeMessage changePrimaryDc(Long clusterDbId, Long shardDbId, String newPrimaryDc, MasterInfo masterInfo) {
		
		try{
			return doChangePrimaryDc(clusterDbId, shardDbId, newPrimaryDc, masterInfo);
		}catch(Exception e){
			executionLog.error(e.getMessage());
			logger.error("[changePrimaryDc]" + clusterDbId + "," + shardDbId + "," + newPrimaryDc, e);
			return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, executionLog.getLog());
		}
	}

	protected abstract PrimaryDcChangeMessage doChangePrimaryDc(Long clusterDbId, Long shardDbId, String newPrimaryDc, MasterInfo masterInfo);

	protected abstract void changeSentinel(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster);

	protected void makeKeepersOk(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster) {

		List<KeeperMeta> keepers = currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId);
		executionLog.info("[makeKeepersOk]" + keepers);
		
		KeeperStateChangeJob job = new KeeperStateChangeJob(keepers,
				new Pair<String, Integer>(newMaster.getKey(), newMaster.getValue()),
				currentMetaManager.randomRoute(clusterDbId),
				keyedObjectPool, 1000, 1, scheduled, executors);
		try {
			job.execute().get(waitTimeoutSeconds/2, TimeUnit.SECONDS);
			executionLog.info("[makeKeepersOk]success");
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("[makeKeepersOk]" + e.getMessage());
			executionLog.info("[makeKeepersOk][fail]" + e.getMessage());
		}
	}

	protected abstract void makeRedisesOk(Pair<String, Integer> newMaster, List<RedisMeta> slaves);

	protected abstract List<RedisMeta> getAllSlaves(Pair<String, Integer> newMaster, List<RedisMeta> shardRedises);

	protected abstract Pair<String, Integer> chooseNewMaster(Long clusterDbId, Long shardDbId);

	protected void doChangeMetaCache(Long clusterDbId, Long shardDbId, String newPrimaryDc) {
		
		executionLog.info(String.format("[doChangeMetaCache]%s %s -> %s", clusterDbId, shardDbId, newPrimaryDc));
		dcMetaCache.primaryDcChanged(clusterDbId, shardDbId, newPrimaryDc);
	}

}
