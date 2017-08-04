package com.ctrip.xpipe.redis.meta.server.dcchange.impl;


import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultSlaveOfCommand;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.dcchange.RedisReadonly;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.dcchange.exception.ChooseNewMasterFailException;
import com.ctrip.xpipe.redis.meta.server.dcchange.exception.MakeRedisMasterFailException;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 * Dec 11, 2016
 */
public class BecomePrimaryAction extends AbstractChangePrimaryDcAction{

	private NewMasterChooser newMasterChooser;

	public BecomePrimaryAction(DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, SentinelManager sentinelManager, XpipeNettyClientKeyedObjectPool keyedObjectPool, NewMasterChooser newMasterChooser, ScheduledExecutorService scheduled, Executor executors) {
		super(dcMetaCache, currentMetaManager, sentinelManager, keyedObjectPool, scheduled, executors);
		this.newMasterChooser = newMasterChooser;
	}

	protected PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		
		doChangeMetaCache(clusterId, shardId, newPrimaryDc);
		
		executionLog.info(String.format("[chooseNewMaster][begin]"));
		Pair<String, Integer> newMaster = chooseNewMaster(clusterId, shardId);
		executionLog.info(String.format("[chooseNewMaster]%s:%d", newMaster.getKey(), newMaster.getValue()));
		
		List<RedisMeta> slaves = getAllSlaves(newMaster, dcMetaCache.getShardRedises(clusterId, shardId));
		
		makeRedisesOk(newMaster, slaves);
		
		makeKeepersOk(clusterId, shardId, newMaster);
		
		changeSentinel(clusterId, shardId, newMaster);
		
		return new PrimaryDcChangeMessage(executionLog.getLog(), newMaster.getKey(), newMaster.getValue());
	}



	@Override
	protected void changeSentinel(String clusterId, String shardId, Pair<String, Integer> newMaster) {
		
		try{
			RedisMeta redisMaster = new RedisMeta().setIp(newMaster.getKey()).setPort(newMaster.getValue());
			sentinelManager.addSentinel(clusterId, shardId, redisMaster, executionLog);
		}catch(Exception e){
			executionLog.error("[addSentinel][fail]" + e.getMessage());
			logger.error("[addSentinel]" + clusterId + "," + shardId, e);
		}
	}

	
	@Override
	protected void makeRedisesOk(Pair<String, Integer> newMaster, List<RedisMeta> slaves) {
		
		executionLog.info("[make redis master]" + newMaster);
		
		SimpleObjectPool<NettyClient> masterPool = keyedObjectPool.getKeyPool(new InetSocketAddress(newMaster.getKey(), newMaster.getValue()));
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
			logger.error("[makeRedisesOk]" + slaves + "->" + newMaster, e);
			executionLog.error("[make slaves slaveof][fail]" + e.getMessage());
			//go on
		}
	}

	@Override
	protected Pair<String, Integer> chooseNewMaster(String clusterId, String shardId) {

		List<RedisMeta> redises = dcMetaCache.getShardRedises(clusterId, shardId);
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
