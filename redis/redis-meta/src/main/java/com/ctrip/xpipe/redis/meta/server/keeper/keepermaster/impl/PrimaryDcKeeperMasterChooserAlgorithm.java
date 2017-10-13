package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public class PrimaryDcKeeperMasterChooserAlgorithm extends AbstractKeeperMasterChooserAlgorithm{
	
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;
	
	private int checkRedisTimeoutSeconds;

	public PrimaryDcKeeperMasterChooserAlgorithm(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, XpipeNettyClientKeyedObjectPool keyedObjectPool, int checkRedisTimeoutSeconds, ScheduledExecutorService scheduled) {
		super(clusterId, shardId, dcMetaCache, currentMetaManager, scheduled);
		this.keyedObjectPool = keyedObjectPool;
		this.checkRedisTimeoutSeconds = checkRedisTimeoutSeconds;
	}

	@Override
	protected Pair<String, Integer> doChoose() {

		Pair<String, Integer> currentMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);
		
		
		List<RedisMeta>  allRedises = dcMetaCache.getShardRedises(clusterId, shardId);
		
		List<RedisMeta>  redisMasters = getMasters(allRedises);
		
		if(redisMasters.size() == 0){

			logger.info("[chooseKeeperMaster][none redis is master, use first redis]{}", allRedises);
			if(allRedises.size() == 0){
				logger.info("[chooseKeeperMaster][no redis]{}", allRedises);
				return null;
			}
			return new Pair<>(allRedises.get(0).getIp(), allRedises.get(0).getPort());
		}else if(redisMasters.size() == 1){
			return new Pair<>(redisMasters.get(0).getIp(), redisMasters.get(0).getPort());
		}else{
			logger.error("[chooseKeeperMaster][multi master]{}, {}", redisMasters.size(), redisMasters);
			for(RedisMeta redisMaster : redisMasters){
				
				Pair<String, Integer> masterPair = new Pair<>(redisMaster.getIp(), redisMaster.getPort());
				if(currentMaster != null && currentMaster.equals(masterPair)){
					logger.info("[chooseKeeperMaster][choose previous master]{}", redisMaster);
					return masterPair;
				}
			}
			logger.info("[chooseKeeperMaster][choose first]{}", redisMasters.get(0));
			return new Pair<>(redisMasters.get(0).getIp(), redisMasters.get(0).getPort());
		}
	}
	
	protected List<RedisMeta> getMasters(List<RedisMeta> allRedises) {
		
		List<RedisMeta> result = new LinkedList<>();
		
		for(RedisMeta redisMeta : allRedises){
			if(isMaster(redisMeta)){
				result.add(redisMeta);
			}
		}
		
		return result;
	}

	protected boolean isMaster(RedisMeta redisMeta) {
		
		try {
			SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort()));
			Role role = new RoleCommand(clientPool, false, scheduled).execute().get(checkRedisTimeoutSeconds, TimeUnit.SECONDS);
			return SERVER_ROLE.MASTER == role.getServerRole();
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("[isMaster]" + redisMeta, e);
		}
		return false;
	}
}
