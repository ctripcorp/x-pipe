package com.ctrip.xpipe.redis.keeper.impl;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.meta.ShardStatus;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServiceManager.MetaUpdateInfo;

/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public abstract class AbstractRedisKeeperServerState implements RedisKeeperServerState{

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private ShardStatus shardStatus;

	protected RedisKeeperServer redisKeeperServer;
	
	public AbstractRedisKeeperServerState(RedisKeeperServer redisKeeperServer){
		this(redisKeeperServer, null);
	}
	
	public AbstractRedisKeeperServerState(RedisKeeperServer redisKeeperServer, ShardStatus shardStatus) {
		
		this.redisKeeperServer = redisKeeperServer;
		this.shardStatus = shardStatus;
	}
	
	@Override
	public Endpoint getMaster() {
		
		if(shardStatus == null){
			return null;
		}
		return doGetMaster(shardStatus);
	}

	protected abstract Endpoint doGetMaster(ShardStatus shardStatus);

	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof MetaUpdateInfo){
			
			logger.info("{}", args);

			MetaUpdateInfo metaUpdateInfo = (MetaUpdateInfo) args;
			if(!redisKeeperServer.getClusterId().equals(metaUpdateInfo.getClusterId()) || !redisKeeperServer.getShardId().equals(metaUpdateInfo.getShardId())){
				return;
			}
			
			Object change = metaUpdateInfo.getInfo();
			if(change instanceof ShardStatus){
				setShardStatus((ShardStatus)change);
			}else{
				throw new IllegalStateException("unkonw info:" + change);
			}
		}
	}

	private void setShardStatus(ShardStatus shardStatus) {
		
		if(shardStatus.getRedisMaster() != null && shardStatus.getUpstreamKeeper() != null){
			logger.error("[setShardStatus][active keeper and upstream keeper both not null]{}, {}", shardStatus.getActiveKeeper(), shardStatus.getUpstreamKeeper());
			return;
		}
		
		ShardStatus old = this.shardStatus;
		
		this.shardStatus =  shardStatus;
		
		KeeperMeta oldActiveKeeperMeta = null, oldUpstreamKeeper = null;
		RedisMeta oldMaterRedisMeta = null;
		
		if(old != null){
			oldActiveKeeperMeta = old.getActiveKeeper();
			oldUpstreamKeeper = old.getUpstreamKeeper();
			oldMaterRedisMeta = old.getRedisMaster();
		}
		
		cmpActiveKeeper(oldActiveKeeperMeta, shardStatus.getActiveKeeper());
		
		
		cmpKeeperMaster(oldUpstreamKeeper, shardStatus.getUpstreamKeeper(), oldMaterRedisMeta, shardStatus.getRedisMaster());
	}

	private void cmpKeeperMaster(KeeperMeta oldUpstreamKeeper, KeeperMeta upstreamKeeper, RedisMeta oldMaterRedisMeta,
			RedisMeta redisMaster) {

		boolean changed = false;

		if(!ObjectUtils.equals(oldUpstreamKeeper,  upstreamKeeper)){
			logger.info("[cmpKeeperMaster][upstream changed]{} --> {}", oldUpstreamKeeper, upstreamKeeper);
			changed = true;
		}
		
		if(!ObjectUtils.equals(oldMaterRedisMeta, redisMaster)){
			logger.info("[cmpKeeperMaster][redis master changed]{} --> {}", oldMaterRedisMeta, redisMaster);
			changed = true;
		}
		
		if(!changed){
			logger.info("[cmpKeeperMaster][not changed]");
			return;
		}
		
		keeperMasterChanged();
	}

	private void cmpActiveKeeper(KeeperMeta oldActiveKeeperMeta, KeeperMeta activeKeeper) {

		if(ObjectUtils.equals(oldActiveKeeperMeta, activeKeeper)){
			logger.info("[cmpActiveKeeper][equal]");
			return;
		}
		
		if(oldActiveKeeperMeta == null){
			logger.info("[gotActiveKeeperInfo][new activeKeeperMeta]{}", activeKeeper);
		}else{
			logger.info("[gotActiveKeeperInfo][activeKeeperMeta changed]{} -> {}", oldActiveKeeperMeta, activeKeeper);
		}
		
		KeeperMeta currentKeeperMeta = redisKeeperServer.getCurrentKeeperMeta();
		
		if(activeKeeper.getIp().equals(currentKeeperMeta.getIp()) && activeKeeper.getPort().equals(currentKeeperMeta.getPort())){
			becomeActive();
		}else{
			becomeBackup();
		}
	}

	protected abstract  void keeperMasterChanged();

	protected abstract void becomeBackup();
	
	protected abstract void becomeActive();

	@Override
	public void setPromotionState(PROMOTION_STATE promotionState) {
		this.setPromotionState(promotionState, null);
	}

	protected void reconnectMaster(){
		this.redisKeeperServer.reconnectMaster();
	}
	
	@Override
	public RedisKeeperServer getRedisKeeperServer() {
		return this.redisKeeperServer;
	}

	protected ShardStatus getShardStatus() {
		return shardStatus;
	}

	@Override
	public String toString() {
		
		return String.format("redisKeeperServer:%s, shardInfo:%s", redisKeeperServer, shardStatus);
	}
	
	@Override
	public boolean sendKinfo() {
		return false;
	}
}
