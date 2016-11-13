package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Nov 4, 2016
 */
public class PrimaryDcKeeperMasterChooser extends AbstractKeeperMasterChooser{

	public PrimaryDcKeeperMasterChooser(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
		super(clusterId, shardId, dcMetaCache, currentMetaManager, scheduled);
	}

	@Override
	protected Pair<String, Integer> chooseKeeperMaster() {
		
		if(!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)){
			
			logger.warn("[chooseKeeperMaster][current dc not primary]{}, {}", clusterId, shardId);
			try {
				stop();
			} catch (Exception e) {
				logger.error("[chooseKeeperMaster]", e);
			}
			return null;
		}
		
		
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
			Role role = new RoleCommand(redisMeta.getIp(), redisMeta.getPort()).execute().get(checkIntervalSeconds/2, TimeUnit.SECONDS);
			return SERVER_ROLE.MASTER == role.getServerRole();
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("[isMaster]" + redisMeta, e);
		}
		return false;
	}

}
