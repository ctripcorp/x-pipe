package com.ctrip.xpipe.redis.meta.server.redis.impl;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;

/**
 * @author wenchao.meng
 *
 * Dec 26, 2016
 */
public class BackupDcClusterRedisStateAjust extends AbstractClusterRedisStateAjustTask{
	
	private String clusterId;
	
	private CurrentMetaManager currentMetaManager;
	
	private XpipeNettyClientKeyedObjectPool pool;
	
	private ScheduledExecutorService scheduled;

	private Executor executors;
	
	public BackupDcClusterRedisStateAjust(String clusterId, CurrentMetaManager currentMetaManager, XpipeNettyClientKeyedObjectPool pool, ScheduledExecutorService scheduled, Executor executors) {
		this.clusterId = clusterId;
		this.currentMetaManager = currentMetaManager;
		this.pool = pool;
		this.scheduled = scheduled;
		this.executors = executors;
	}
	

	@Override
	protected void doRun() throws Exception {

		ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterId);
		if(clusterMeta == null){
			logger.warn("[doRun][cluster null]{}", clusterId);
			return;
		}
		
		for(ShardMeta shardMeta : clusterMeta.getShards().values()){
			
			logger.debug("[doRun]{}, {}", clusterId, shardMeta.getId());
			KeeperMeta keeperActive = currentMetaManager.getKeeperActive(clusterId, shardMeta.getId());
			if(keeperActive == null){
				logger.debug("[doRun][keeper active null]{}, {}", clusterId, shardMeta.getId());
				continue;
			}
			List<RedisMeta> redisesNeedChange = getRedisesNeedToChange(shardMeta, keeperActive);
			
			if(redisesNeedChange.size() == 0){
				continue;
			}
			
			logger.info("[doRun][change state]{}, {}", keeperActive, redisesNeedChange);
			new DefaultSlaveOfJob(redisesNeedChange, keeperActive.getIp(), keeperActive.getPort(), pool, scheduled, executors).
			execute().addListener(new CommandFutureListener<Void>() {
				
				@Override
				public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
					if(!commandFuture.isSuccess()){
						logger.error("[operationComplete][fail]" + commandFuture.command(), commandFuture.cause());
					}
				}
			});
		}
	}

	protected List<RedisMeta> getRedisesNeedToChange(ShardMeta shardMeta, KeeperMeta keeperActive) {
		
		List<RedisMeta> redisesNeedChange = new LinkedList<>();

		for(RedisMeta redisMeta : shardMeta.getRedises()){
			
			try{
				boolean change = false;
				RoleCommand roleCommand = new RoleCommand(pool.getKeyPool(new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort())), false, scheduled);
				Role role = roleCommand.execute().get();
				
				if(role.getServerRole() == SERVER_ROLE.MASTER){
					change = true;
					logger.info("[getRedisesNeedToChange][redis master, change to slave of keeper]{}, {}", redisMeta, keeperActive);
				}else if(role.getServerRole() == SERVER_ROLE.SLAVE){
					SlaveRole slaveRole = (SlaveRole) role;
					if(!keeperActive.getIp().equals(slaveRole.getMasterHost())  || !keeperActive.getPort().equals(slaveRole.getMasterPort())){
						logger.info("[getRedisesNeedToChange][redis master not active keeper, change to slaveof keeper]{}, {}, {}", slaveRole, redisMeta, keeperActive);
						change = true;
					}
				}else{
					logger.warn("[doRun][role error]{}, {}", redisMeta, role);
					continue;
				}
				if(change){
					redisesNeedChange.add(redisMeta);
				}
			}catch(Exception e){
				logger.error("[doRun]" + redisMeta, e);
			}
		}
		return redisesNeedChange;
	}
}
