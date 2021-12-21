package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJob;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.BackupDcKeeperMasterChooserAlgorithm;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 11, 2016
 */
public class BecomeBackupAction extends AbstractChangePrimaryDcAction{

	private MultiDcService multiDcService;
	
	public BecomeBackupAction(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, SentinelManager sentinelManager, ExecutionLog executionLog,
							  XpipeNettyClientKeyedObjectPool keyedObjectPool,
							  MultiDcService multiDcService, ScheduledExecutorService scheduled, Executor executors) {
		super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, sentinelManager, executionLog, keyedObjectPool, scheduled, executors);
		this.multiDcService = multiDcService;
	}
	
	@Override
	protected PrimaryDcChangeMessage doChangePrimaryDc(Long clusterDbId, Long shardDbId, String newPrimaryDc, MasterInfo masterInfo) {
		
		doChangeMetaCache(clusterDbId, shardDbId, newPrimaryDc);

		changeSentinel(clusterDbId, shardDbId, null);

		Pair<String, Integer> newMaster = chooseNewMaster(clusterDbId, shardDbId);
		if(newMaster == null){
			executionLog.error("[doChangePrimaryDc][new master null]");
			return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, executionLog.getLog());
		}
		executionLog.info(String.format("[chooseNewMaster]%s:%d", newMaster.getKey(), newMaster.getValue()));
		
		makeKeepersOk(clusterDbId, shardDbId, newMaster);

		List<RedisMeta> slaves = getAllSlaves(newMaster, dcMetaCache.getShardRedises(clusterDbId, shardDbId));

		KeeperMeta activeKeeper = currentMetaManager.getKeeperActive(clusterDbId, shardDbId);
		makeRedisesOk(new Pair<>(activeKeeper.getIp(), activeKeeper.getPort()), slaves);
		
		return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.SUCCESS, executionLog.getLog());
	}

	@Override
	protected Pair<String, Integer> chooseNewMaster(Long clusterDbId, Long shardDbId) {
		
		BackupDcKeeperMasterChooserAlgorithm algorithm = new BackupDcKeeperMasterChooserAlgorithm(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, multiDcService, scheduled);
		return algorithm.choose();
	}


	@Override
	protected void changeSentinel(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster) {
		executionLog.info("[changeSentinel][nothing need to be done]");
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
