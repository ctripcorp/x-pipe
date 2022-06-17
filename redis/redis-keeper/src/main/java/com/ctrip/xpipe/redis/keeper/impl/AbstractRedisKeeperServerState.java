package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public abstract class AbstractRedisKeeperServerState implements RedisKeeperServerState{

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected Endpoint masterAddress;
	
	protected RedisKeeperServer redisKeeperServer;
	
	public AbstractRedisKeeperServerState(RedisKeeperServer redisKeeperServer){
		this(redisKeeperServer, null);
	}

	public AbstractRedisKeeperServerState(RedisKeeperServer redisKeeperServer, Endpoint masterAddress){
		
		this.redisKeeperServer = redisKeeperServer;
		this.masterAddress = masterAddress;
	}

	@Override
	public Endpoint getMaster() {
		return masterAddress;
	}


	
	@Override
	public void setShardStatus(ShardStatus shardStatus) throws IOException {
		
		if(shardStatus.getRedisMaster() != null && shardStatus.getUpstreamKeeper() != null){
			logger.error("[setShardStatus][active keeper and upstream keeper both not null]{}, {}", shardStatus.getActiveKeeper(), shardStatus.getUpstreamKeeper());
			return;
		}

		KeeperMeta currentKeeperMeta = redisKeeperServer.getCurrentKeeperMeta();
		
		KeeperMeta activeKeeper = shardStatus.getActiveKeeper();
		
		if(activeKeeper == null){
			logger.info("[setShardStatus][active keeper null]");
			return;
		}
		
		if(activeKeeper.getIp().equals(currentKeeperMeta.getIp()) && activeKeeper.getPort().equals(currentKeeperMeta.getPort())){
			RedisMeta redisMaster = shardStatus.getRedisMaster();
			KeeperMeta upstreamKeeer = shardStatus.getUpstreamKeeper();
			
			Endpoint masterAddress = null;
			if(redisMaster != null){
				masterAddress = new DefaultEndPoint(redisMaster.getIp(), redisMaster.getPort());
			}
			
			if(upstreamKeeer != null){
				masterAddress = new DefaultEndPoint(upstreamKeeer.getIp(), upstreamKeeer.getPort());
			}
			becomeActive(masterAddress);
		}else{
			becomeBackup(new DefaultEndPoint(activeKeeper.getIp(), activeKeeper.getPort()));
		}
	}

	@Override
	public void setMasterAddress(Endpoint masterAddress) {
		
		if(ObjectUtils.equals(this.masterAddress, masterAddress)){

			/* TODO: remove ProxyEnabled */
			if(this.masterAddress instanceof ProxyEnabled) {
				ProxyEnabled current = (ProxyEnabled) this.masterAddress;
				ProxyEnabled future = (ProxyEnabled) masterAddress;
				if(current.isSameWith(future)) {
					logger.info("[setMasterAddress][proxied][master address unchanged]{},{}", this.masterAddress, masterAddress);
					return;
				}
			} else {
				logger.info("[setMasterAddress][master address unchanged]{},{}", this.masterAddress, masterAddress);
				return;
			}
		}
		logger.info("[setMasterAddress]{}, {}", this.masterAddress, masterAddress);
		this.masterAddress = masterAddress;
		keeperMasterChanged();
		
	}


	protected abstract  void keeperMasterChanged();

	@Override
	public void setPromotionState(PROMOTION_STATE promotionState) throws IOException {
		this.setPromotionState(promotionState, null);
	}

	protected void reconnectMaster(){
		logger.info("[reconnectMaster]{}", redisKeeperServer);
		this.redisKeeperServer.reconnectMaster();
	}
	
	@Override
	public RedisKeeperServer getRedisKeeperServer() {
		return this.redisKeeperServer;
	}

	@Override
	public String toString() {
		
		return String.format("state:%s, master:%s", keeperState(), getMaster());
	}

	@Override
	public void initPromotionState() {
		logger.info("[initPromotionState][nothing to do]");
	}
	
	protected void doBecomeBackup(Endpoint masterAddress){
		
		logger.info("[doBecomeBackup]{}", this);
		try{
			redisKeeperServer.getReplicationStore().getMetaStore().becomeBackup();
		}catch(Exception e){
			logger.error("[activedoBecomeBackupToBackup]" + this, e);
		}
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateBackup(redisKeeperServer, masterAddress));
		reconnectMaster();
	}

	protected void doBecomeActive(Endpoint masterAddress){
		
		logger.info("[doBecomeActive]{}", this);
		try{
			ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
			replicationStore.getMetaStore().becomeActive();
		}catch(Exception e){
			logger.error("[doBecomeActive]" + this + "," + masterAddress, e);
		}
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, masterAddress));
		reconnectMaster();
	}

	@Override
	public boolean handleSlaveOf() {
		return false;
	}
}
