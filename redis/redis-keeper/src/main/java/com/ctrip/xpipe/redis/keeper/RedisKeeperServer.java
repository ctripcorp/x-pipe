package com.ctrip.xpipe.redis.keeper;


import java.io.IOException;
import java.util.Set;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;

import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:09:23
 */
public interface RedisKeeperServer extends RedisServer, PsyncObserver{
	
	int getListeningPort();
	
	KeeperRepl getKeeperRepl();
	
	RedisClient clientConnected(Channel channel);
	
	void clientDisConnected(Channel channel);
	
	String getKeeperRunid();
	
	void readRdbFile(RdbFileListener rdbFileListener) throws IOException;
	
	/**
	 * include all client roles
	 * @return
	 */
	Set<RedisClient> allClients();
	
	Set<RedisSlave> slaves();
		
	ReplicationStore getReplicationStore();
		
	String getClusterId();
	
	String getShardId();
	
	void setRedisKeeperServerState(RedisKeeperServerState redisKeeperServerState);
	
	RedisKeeperServerState getRedisKeeperServerState();
	
	KeeperMeta getCurrentKeeperMeta();
	
	void reconnectMaster();
	
	void stopAndDisposeMaster();
	
	RedisMaster getRedisMaster();
	
	void promoteSlave(String ip, int port) throws RedisSlavePromotionException;
	
	public static enum PROMOTION_STATE{
		
		NORMAL,
		BEGIN_PROMOTE_SLAVE,//promote slave to master. 1.should not receive commands, 2. disconnect with master
		SLAVE_PROMTED,
		REPLICATION_META_EXCHANGED
		
	}
}
