package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.util.List;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public abstract class AbstractChangePrimaryDcAction implements ChangePrimaryDcAction{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected static final int DEFAULT_CHANGE_PRIMARY_WAIT_TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("DEFAULT_CHANGE_PRIMARY_WAIT_TIMEOUT_SECONDS", "10")); 
	
	protected int waitTimeoutSeconds = DEFAULT_CHANGE_PRIMARY_WAIT_TIMEOUT_SECONDS;
	
	protected ExecutionLog executionLog = new ExecutionLog();
	
	protected DcMetaCache   dcMetaCache;
	
	protected CurrentMetaManager currentMetaManager;
	
	protected SentinelManager sentinelManager;
	
	protected XpipeNettyClientKeyedObjectPool keyedObjectPool;
	
	protected ScheduledExecutorService scheduled;

	protected Executor executors;

	public AbstractChangePrimaryDcAction(DcMetaCache dcMetaCache,
										 CurrentMetaManager currentMetaManager,
										 SentinelManager sentinelManager,
										 XpipeNettyClientKeyedObjectPool keyedObjectPool,
										 ScheduledExecutorService scheduled,
										 Executor executors) {
		this.dcMetaCache = dcMetaCache;
		this.currentMetaManager = currentMetaManager;
		this.sentinelManager = sentinelManager;
		this.keyedObjectPool = keyedObjectPool;
		this.scheduled = scheduled;
		this.executors = executors;
	}

	@Override
	public PrimaryDcChangeMessage changePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		
		try{
			return doChangePrimaryDc(clusterId, shardId, newPrimaryDc);
		}catch(Exception e){
			executionLog.error(e.getMessage());
			logger.error("[changePrimaryDc]" + clusterId + "," + shardId + "," + newPrimaryDc, e);
			return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, executionLog.getLog());
		}
	}

	protected abstract PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc);

	protected abstract void changeSentinel(String clusterId, String shardId, Pair<String, Integer> newMaster);

	protected void makeKeepersOk(String clusterId, String shardId, Pair<String, Integer> newMaster) {

		List<KeeperMeta> keepers = currentMetaManager.getSurviveKeepers(clusterId, shardId);
		executionLog.info("[makeKeepersOk]" + keepers);
		
		KeeperStateChangeJob job = new KeeperStateChangeJob(keepers, 
				new Pair<String, Integer>(newMaster.getKey(), newMaster.getValue()), 
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

	protected abstract Pair<String, Integer> chooseNewMaster(String clusterId, String shardId);

	protected void doChangeMetaCache(String clusterId, String shardId, String newPrimaryDc) {
		
		executionLog.info(String.format("[doChangeMetaCache]%s %s -> %s", clusterId, shardId, newPrimaryDc));
		dcMetaCache.primaryDcChanged(clusterId, shardId, newPrimaryDc);
	}

}
