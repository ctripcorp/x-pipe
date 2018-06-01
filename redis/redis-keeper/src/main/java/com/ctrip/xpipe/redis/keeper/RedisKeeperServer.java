package com.ctrip.xpipe.redis.keeper;


import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.impl.SetRdbDumperException;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:09:23
 */
public interface RedisKeeperServer extends RedisServer, PsyncObserver, Destroyable{
	
	int getListeningPort();
	
	KeeperRepl getKeeperRepl();
	
	RedisClient clientConnected(Channel channel);
	
	void clientDisConnected(Channel channel);
	
	String getKeeperRunid();
	
	/**
	 * include all client roles
	 * @return
	 */
	Set<RedisClient> allClients();
	
	Set<RedisSlave> slaves();
		
	ReplicationStore getReplicationStore();
		
	String getClusterId();
	
	String getShardId();
	
	boolean compareAndDo(RedisKeeperServerState expected, Runnable action);
	
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

	void fullSyncToSlave(RedisSlave redisSlave) throws IOException;
	
	KeeperInstanceMeta getKeeperInstanceMeta();
	
	KeeperConfig getKeeperConfig();

	void clearRdbDumper(RdbDumper rdbDumper);
	
	void setRdbDumper(RdbDumper rdbDumper) throws SetRdbDumperException;

	void setRdbDumper(RdbDumper rdbDumper, boolean force) throws SetRdbDumperException;
	
	RdbDumper rdbDumper();
	
	KeeperMonitor getKeeperMonitor();

	void processCommandSequentially(Runnable runnable);
}
