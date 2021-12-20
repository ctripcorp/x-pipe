package com.ctrip.xpipe.redis.meta.server.dcchange.impl;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultSlaveOfCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.dcchange.*;
import com.ctrip.xpipe.redis.meta.server.dcchange.exception.ChooseNewMasterFailException;
import com.ctrip.xpipe.redis.meta.server.dcchange.exception.MakeRedisMasterFailException;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *
 * Dec 11, 2016
 */
public class BecomePrimaryAction extends AbstractChangePrimaryDcAction{

	private NewMasterChooser newMasterChooser;
	private OffsetWaiter offsetWaiter;

	public BecomePrimaryAction(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
							   SentinelManager sentinelManager, OffsetWaiter offsetWaiter, ExecutionLog executionLog,
							   XpipeNettyClientKeyedObjectPool keyedObjectPool, NewMasterChooser newMasterChooser,
							   ScheduledExecutorService scheduled, Executor executors) {
		super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, sentinelManager, executionLog, keyedObjectPool, scheduled, executors);
		this.newMasterChooser = newMasterChooser;
		this.offsetWaiter = offsetWaiter;
	}

	protected PrimaryDcChangeMessage doChangePrimaryDc(Long clusterDbId, Long shardDbId, String newPrimaryDc, MasterInfo masterInfo) {
		
		doChangeMetaCache(clusterDbId, shardDbId, newPrimaryDc);
		
		executionLog.info(String.format("[chooseNewMaster][begin]"));
		Pair<String, Integer> newMaster = chooseNewMaster(clusterDbId, shardDbId);
		executionLog.info(String.format("[chooseNewMaster]%s:%d", newMaster.getKey(), newMaster.getValue()));

		//wait for slave to achieve master offset
		offsetWaiter.tryWaitfor(new HostPort(newMaster.getKey(), newMaster.getValue()), masterInfo, executionLog);

		List<RedisMeta> slaves = getAllSlaves(newMaster, dcMetaCache.getShardRedises(clusterDbId, shardDbId));
		
		makeRedisesOk(newMaster, slaves);
		
		makeKeepersOk(clusterDbId, shardDbId, newMaster);
		
		changeSentinel(clusterDbId, shardDbId, newMaster);
		
		return new PrimaryDcChangeMessage(executionLog.getLog(), newMaster.getKey(), newMaster.getValue());
	}



	@Override
	protected void changeSentinel(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster) {
		
		try{
			sentinelManager.addSentinel(clusterDbId, shardDbId, HostPort.fromPair(newMaster), executionLog);
		}catch(Exception e){
			executionLog.error("[addSentinel][fail]" + e.getMessage());
			logger.error("[addSentinel]" + clusterDbId + "," + shardDbId, e);
		}
	}

	
	@Override
	protected void makeRedisesOk(Pair<String, Integer> newMaster, List<RedisMeta> slaves) {
		
		executionLog.info("[make redis master]" + newMaster);
		
		SimpleObjectPool<NettyClient> masterPool = keyedObjectPool.getKeyPool(new DefaultEndPoint(newMaster.getKey(), newMaster.getValue()));
		Command<String> command = new DefaultSlaveOfCommand(masterPool, null, 0, scheduled);
		try {
			String result = command.execute().get();
			executionLog.info("[make redis master]" + result);
			RedisReadonly redisReadOnly = RedisReadonly.create(newMaster.getKey(), newMaster.getValue(), keyedObjectPool, scheduled);
			if(!(redisReadOnly instanceof SlaveOfRedisReadOnly)){
				redisReadOnly.makeWritable();
			}
		} catch (Exception e) {
			logger.error("[makeRedisesOk]" + newMaster, e);
			executionLog.error("[make redis master fail]" + e.getMessage());
			throw new MakeRedisMasterFailException("make redis master:" + newMaster, e);
		}
		
		executionLog.info("[make slaves slaveof][begin]" + newMaster + "," + slaves);
		Command<Void> slavesJob = new DefaultSlaveOfJob(slaves, newMaster.getKey(), newMaster.getValue(), keyedObjectPool, scheduled, executors);
		try {
			slavesJob.execute().get(waitTimeoutSeconds, TimeUnit.SECONDS);
			executionLog.info("[make slaves slaveof]success");
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			String failMsg = StringUtil.isEmpty(e.getMessage()) ? e.getClass().getName() : e.getMessage();
			EventMonitor.DEFAULT.logAlertEvent(String.format("[mig][slaves][fail][%d][%d] %s", cluster, shard, slavesJob.toString()));
			logger.error("[makeRedisesOk][fail][ignore]" + slaves + "->" + newMaster, e);
			executionLog.error("[make slaves slaveof][fail][ignore]" + failMsg);
		}
	}

	@Override
	protected Pair<String, Integer> chooseNewMaster(Long clusterDbId, Long shardDbId) {

		List<RedisMeta> redises = dcMetaCache.getShardRedises(clusterDbId, shardDbId);
		String desc = MetaUtils.toString(redises);
		executionLog.info("[chooseNewMaster][from]" + desc);
		RedisMeta newMaster = newMasterChooser.choose(redises);
		if(newMaster == null){
			throw ChooseNewMasterFailException.chooseNull(redises);
		}
		return new Pair<>(newMaster.getIp(), newMaster.getPort());
	}

	@Override
	protected List<RedisMeta> getAllSlaves(Pair<String, Integer> newMaster, List<RedisMeta> shardRedises) {
		
		List<RedisMeta> result = new LinkedList<>();
		
		Iterator<RedisMeta> iterator = shardRedises.iterator();
		while(iterator.hasNext()){
			RedisMeta current = iterator.next();
			if(ObjectUtils.equals(current.getIp(), newMaster.getKey()) && ObjectUtils.equals(current.getPort(), newMaster.getValue())){
				continue;
			}
			result.add(current);
		}
		return result;
	}

}
