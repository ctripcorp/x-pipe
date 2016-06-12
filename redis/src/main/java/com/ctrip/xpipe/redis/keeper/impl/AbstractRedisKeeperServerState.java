package com.ctrip.xpipe.redis.keeper.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServiceManager.MetaUpdateInfo;

/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public abstract class AbstractRedisKeeperServerState implements RedisKeeperServerState{

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private KeeperMeta activeKeeperMeta;
	
	private RedisMeta  masterRedisMeta;
	
	protected RedisKeeperServer redisKeeperServer;
	
	public AbstractRedisKeeperServerState(RedisKeeperServer redisKeeperServer){
		this(redisKeeperServer, null, null);
	}
	
	public AbstractRedisKeeperServerState(RedisKeeperServer redisKeeperServer, KeeperMeta  activeKeeperMeta, RedisMeta masterRedisMeta) {
		
		this.redisKeeperServer = redisKeeperServer;
		this.activeKeeperMeta = activeKeeperMeta;
		this.masterRedisMeta = masterRedisMeta;
	}

	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof MetaUpdateInfo){
			
			MetaUpdateInfo metaUpdateInfo = (MetaUpdateInfo) args;
			if(!redisKeeperServer.getClusterId().equals(metaUpdateInfo.getClusterId()) || !redisKeeperServer.getShardId().equals(metaUpdateInfo.getShardId())){
				return;
			}
			
			Object change = metaUpdateInfo.getInfo();
			if(change instanceof RedisMeta){
				setRedisMasterMeta((RedisMeta)change);
			}else if(change instanceof KeeperMeta){
				setActiveKeeperMeta((KeeperMeta)change);
			}else{
				throw new IllegalStateException("unkonw info:" + change);
			}
		}
	}

	protected void setActiveKeeperMeta(KeeperMeta activeKeeperMeta) {
		
		if(this.activeKeeperMeta != null && this.activeKeeperMeta.equals(activeKeeperMeta)){
			return;
		}
		
		if(this.activeKeeperMeta == null){
			logger.info("[gotActiveKeeperInfo][new activeKeeperMeta]{}", activeKeeperMeta);
		}else{
			logger.info("[gotActiveKeeperInfo][activeKeeperMeta changed]{} -> {}", this.activeKeeperMeta, activeKeeperMeta);
		}
		
		this.activeKeeperMeta = activeKeeperMeta;
		
		KeeperMeta currentKeeperMeta = redisKeeperServer.getCurrentKeeperMeta();
		
		if(this.activeKeeperMeta.getIp().equals(currentKeeperMeta.getIp()) && this.activeKeeperMeta.getPort().equals(currentKeeperMeta.getPort())){
			becomeActive();
		}else{
			becomeBackup();
		}
	}
		
	protected abstract void becomeBackup();
	
	protected abstract void becomeActive();

	protected void setRedisMasterMeta(RedisMeta clusterRedisMasterMeta) {
		
		if(this.masterRedisMeta != null && this.masterRedisMeta.equals(clusterRedisMasterMeta)){
			return;
		}
		
		RedisMeta old = this.masterRedisMeta;
		if(old == null){
			logger.info("[gotRedisMasterInfo][new master]{}", clusterRedisMasterMeta);
		}else{
			//TODO ask keepermaster again about redis master information
			logger.info("[gotRedisMasterInfo][master changed]{} -> {}", old, clusterRedisMasterMeta);
		}
		
		this.masterRedisMeta  = clusterRedisMasterMeta;
		masterRedisMetaChanged();

	}


	@Override
	public void setPromotionState(PROMOTION_STATE promotionState) {
		this.setPromotionState(promotionState, null);
	}
	
	
	protected abstract void masterRedisMetaChanged();

	@Override
	public RedisKeeperServer getRedisKeeperServer() {
		return this.redisKeeperServer;
	}
	
	public KeeperMeta getActiveKeeperMeta() {
		return activeKeeperMeta;
	}
	
	
	public RedisMeta getMasterRedisMeta() {
		return masterRedisMeta;
	}

	@Override
	public String toString() {
		
		return String.format("%s, active:%s, master:%s", redisKeeperServer.getCurrentKeeperMeta(), activeKeeperMeta, masterRedisMeta);
	}
	
	@Override
	public boolean sendKinfo() {
		return false;
	}
}
