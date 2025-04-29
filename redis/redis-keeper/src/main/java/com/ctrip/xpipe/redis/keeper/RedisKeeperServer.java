package com.ctrip.xpipe.redis.keeper;


import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSyncObserver;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.XSyncContinue;
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
public interface RedisKeeperServer extends RedisServer, GapAllowedSyncObserver, Destroyable{
	
	int getListeningPort();
	
	KeeperRepl getKeeperRepl();

	XSyncContinue locateContinueGtidSet(GtidSet gtidSet) throws Exception;

	void switchToPSync(String replId, long replOff) throws IOException;

	void switchToXSync(String replId, long replOff, String masterUuid, GtidSet gtidCont) throws IOException;

	void clientDisconnected(Channel channel);
	
	String getKeeperRunid();
	
	/**
	 * include all client roles
	 * @return
	 */
	Set<RedisClient> allClients();
	
	Set<RedisSlave> slaves();
		
	ReplicationStore getReplicationStore();

	ReplId getReplId();

	boolean compareAndDo(RedisKeeperServerState expected, Runnable action);
	
	void setRedisKeeperServerState(RedisKeeperServerState redisKeeperServerState);
	
	RedisKeeperServerState getRedisKeeperServerState();

	boolean gapAllowSyncEnabled();
	
	KeeperMeta getCurrentKeeperMeta();
	
	void reconnectMaster();
	
	void stopAndDisposeMaster();
	
	RedisMaster getRedisMaster();
	
	void promoteSlave(String ip, int port) throws RedisSlavePromotionException;

	void closeSlaves(String reason);
	
	public static enum PROMOTION_STATE{
		
		NORMAL,
		BEGIN_PROMOTE_SLAVE,//promote slave to master. 1.should not receive commands, 2. disconnect with master
		SLAVE_PROMTED,
		REPLICATION_META_EXCHANGED
		
	}

	default void fullSyncToSlave(RedisSlave redisSlave) throws IOException {
		getKeeperMonitor().getKeeperStats().increaseFullSync();
		fullSyncToSlave(redisSlave, false);
	}

	void fullSyncToSlave(RedisSlave redisSlave, boolean freshRdbNeeded) throws IOException;

	void startIndexing() throws IOException;

	boolean isStartIndexing();
	
	KeeperInstanceMeta getKeeperInstanceMeta();
	
	KeeperConfig getKeeperConfig();

	void clearRdbDumper(RdbDumper rdbDumper, boolean forceRdb);
	
	void setRdbDumper(RdbDumper rdbDumper) throws SetRdbDumperException;

	void setRdbDumper(RdbDumper rdbDumper, boolean force) throws SetRdbDumperException;
	
	RdbDumper rdbDumper();
	
	KeeperMonitor getKeeperMonitor();

	void resetDefaultReplication();

	PsyncObserver createPsyncObserverForRdbOnlyRepl();

	void resetElection();

	boolean isLeader();

	long getLastElectionResetTime();

	void releaseRdb() throws IOException;

}
