package com.ctrip.xpipe.redis.keeper;



import java.io.IOException;
import java.util.Set;

import com.ctrip.xpipe.redis.protocal.CommandRequester;
import com.ctrip.xpipe.redis.protocal.PsyncObserver;

import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:09:23
 */
public interface RedisKeeperServer extends RedisServer, PsyncObserver{
	
	long getBeginReploffset();

	long getEndReploffset();

	void masterConnected(Channel channel);
	
	void masterDisconntected(Channel channel);
	
	RedisClient clientConnected(Channel channel);
	
	void clientDisConnected(Channel channel);
	
	String getKeeperRunid();
	
	void addCommandsListener(Long offset, CommandsListener listener);

	void readRdbFile(RdbFileListener rdbFileListener) throws IOException;
	
	/**
	 * include all client roles
	 * @return
	 */
	Set<RedisClient> allClients();
	
	Set<RedisSlave> slaves();
	
	CommandRequester getCommandRequester();
	
	ReplicationStore getReplicationStore();
	
	void setKeeperServerState(KEEPER_STATE keeperState);
	
	void setKeeperServerState(KEEPER_STATE keeperState, Object info);
	
	public static enum KEEPER_STATE{
		
		NORMAL,
		BEGIN_PROMOTE_SLAVE,//promote slave to master. 1.should not receive commands, 2. disconnect with master
		COMMANDS_SEND_FINISH,
		SLAVE_PROMTED
	}
}
