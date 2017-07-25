package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJob;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.BackupDcKeeperMasterChooserAlgorithm;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;

/**
 * @author wenchao.meng
 *
 * Dec 11, 2016
 */
public class BecomeBackupAction extends AbstractChangePrimaryDcAction{

	private MultiDcService multiDcService;
	
	public BecomeBackupAction(DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, SentinelManager sentinelManager, XpipeNettyClientKeyedObjectPool keyedObjectPool,
							  MultiDcService multiDcService, ScheduledExecutorService scheduled, Executor executors) {
		super(dcMetaCache, currentMetaManager, sentinelManager, keyedObjectPool, scheduled, executors);
		this.multiDcService = multiDcService;
	}
	
	@Override
	protected PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		
		doChangeMetaCache(clusterId, shardId, newPrimaryDc);

		changeSentinel(clusterId, shardId, null);

		Pair<String, Integer> newMaster = chooseNewMaster(clusterId, shardId);
		if(newMaster == null){
			executionLog.error("[doChangePrimaryDc][new master null]");
			return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, executionLog.getLog());
		}
		executionLog.info(String.format("[chooseNewMaster]%s:%d", newMaster.getKey(), newMaster.getValue()));
		
		makeKeepersOk(clusterId, shardId, newMaster);

		List<RedisMeta> slaves = getAllSlaves(newMaster, dcMetaCache.getShardRedises(clusterId, shardId));

		KeeperMeta activeKeeper = currentMetaManager.getKeeperActive(clusterId, shardId);
		makeRedisesOk(new Pair<>(activeKeeper.getIp(), activeKeeper.getPort()), slaves);
		
		return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.SUCCESS, executionLog.getLog());
	}

	@Override
	protected Pair<String, Integer> chooseNewMaster(String clusterId, String shardId) {
		
		BackupDcKeeperMasterChooserAlgorithm algorithm = new BackupDcKeeperMasterChooserAlgorithm(clusterId, shardId, dcMetaCache, currentMetaManager, multiDcService, scheduled);
		return algorithm.choose();
	}


	@Override
	protected void changeSentinel(String clusterId, String shardId, Pair<String, Integer> newMaster) {
		try{
			
			sentinelManager.removeSentinel(clusterId, shardId, executionLog);
		}catch(Exception e){
			logger.error("[changeSentinel]" + clusterId + "," + shardId, e);
			executionLog.error("[changeSentinel]" + e.getMessage());
		}
	}

	@Override
	protected void makeRedisesOk(Pair<String, Integer> newMaster, List<RedisMeta> slaves) {
		
		try {
			executionLog.info("[makeRedisesOk]" + slaves + "->" + newMaster);
			Command<Void> command = new DefaultSlaveOfJob(slaves, newMaster.getKey(), newMaster.getValue(), keyedObjectPool, scheduled, executors);
			command.execute().get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("[makeRedisesOk]" + slaves, e);
			executionLog.info("[makeRedisesOk][fail]" + e.getMessage());
		}
	}

	@Override
	protected List<RedisMeta> getAllSlaves(Pair<String, Integer> newMaster, List<RedisMeta> shardRedises) {
		return shardRedises;
	}

}
