package com.ctrip.xpipe.redis.keeper.impl;




import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;

/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public abstract class AbstractRedisKeeperServerState implements RedisKeeperServerState{

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected InetSocketAddress masterAddress;
	
	protected RedisKeeperServer redisKeeperServer;
	
	public AbstractRedisKeeperServerState(RedisKeeperServer redisKeeperServer){
		this(redisKeeperServer, null);
	}

	public AbstractRedisKeeperServerState(RedisKeeperServer redisKeeperServer, InetSocketAddress masterAddress){
		
		this.redisKeeperServer = redisKeeperServer;
		this.masterAddress = masterAddress;
	}

	@Override
	public Endpoint getMaster() {
		
		if(masterAddress == null){
			return null;
		}
		return new DefaultEndPoint(masterAddress);
	}


	
	@Override
	public void setShardStatus(ShardStatus shardStatus) {
		
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
			
			InetSocketAddress masterAddress = null;
			if(redisMaster != null){
				masterAddress = new InetSocketAddress(redisMaster.getIp(), redisMaster.getPort());
			}
			
			if(upstreamKeeer != null){
				masterAddress = new InetSocketAddress(upstreamKeeer.getIp(), upstreamKeeer.getPort());
			}
			becomeActive(masterAddress);
		}else{
			becomeBackup(new InetSocketAddress(activeKeeper.getIp(), activeKeeper.getPort()));
		}
	}

	@Override
	public void setMasterAddress(InetSocketAddress masterAddress) {
		
		if(ObjectUtils.equals(this.masterAddress, masterAddress)){
			logger.info("[setMasterAddress][master address unchanged]{},{}", this.masterAddress, masterAddress);
			return;
		}
		this.masterAddress = masterAddress;
		keeperMasterChanged();
		
	}


	protected abstract  void keeperMasterChanged();

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

	@Override
	public String toString() {
		
		return String.format("redisKeeperServer:%s, shardInfo:%s", redisKeeperServer, getMaster());
	}

	@Override
	public void initPromotionState() {
		logger.info("[initPromotionState][nothing to do]");
	}
	
	@Override
	public boolean sendKinfo() {
		return false;
	}
}
