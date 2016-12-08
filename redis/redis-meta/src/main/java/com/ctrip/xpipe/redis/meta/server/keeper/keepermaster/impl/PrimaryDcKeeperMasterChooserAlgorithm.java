package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public class PrimaryDcKeeperMasterChooserAlgorithm extends AbstractKeeperMasterChooserAlgorithm{
	
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;
	
	private int checkRedisTimeoutSeconds;

	public PrimaryDcKeeperMasterChooserAlgorithm(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, XpipeNettyClientKeyedObjectPool keyedObjectPool, int checkRedisTimeoutSeconds) {
		super(clusterId, shardId, dcMetaCache, currentMetaManager);
		this.keyedObjectPool = keyedObjectPool;
		this.checkRedisTimeoutSeconds = checkRedisTimeoutSeconds;
	}

	@Override
	protected Pair<String, Integer> doChoose() {

		Pair<String, Integer> currentMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);
		
		
		List<RedisMeta>  allRedises = dcMetaCache.getShardRedises(clusterId, shardId); 
		List<Pair<String, Integer>>  redisMasters = new LinkedList<>();
		//TODO setinel change event listen...
		for(RedisMeta redisMeta : allRedises){
			if(isMaster(redisMeta)){
				redisMasters.add(new Pair<String, Integer>(redisMeta.getIp(), redisMeta.getPort()));
			}
		}
		
		if(redisMasters.size() == 0){
			logger.info("[chooseKeeperMaster][none redis is master]{}", allRedises);
			return null;
		}else if(redisMasters.size() == 1){
			return redisMasters.get(0);
		}else{
			logger.error("[chooseKeeperMaster][multi master]{}, {}", redisMasters.size(), redisMasters);
			for(Pair<String, Integer> redisMaster : redisMasters){
				if(currentMaster != null && currentMaster.equals(redisMaster)){
					logger.info("[chooseKeeperMaster][choose previous master]{}", redisMaster);
					return redisMaster;
				}
			}
			logger.info("[chooseKeeperMaster][choose first]{}", redisMasters.get(0));
			return redisMasters.get(0);
		}
	}
	
	protected boolean isMaster(RedisMeta redisMeta) {
		try {
			SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort()));
			Role role = new RoleCommand(clientPool).execute().get(checkRedisTimeoutSeconds, TimeUnit.SECONDS);
			return SERVER_ROLE.MASTER == role.getServerRole();
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("[isMaster]" + redisMeta, e);
		}
		return false;
	}
}
