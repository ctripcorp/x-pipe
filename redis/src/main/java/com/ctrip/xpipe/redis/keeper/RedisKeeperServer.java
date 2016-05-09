package com.ctrip.xpipe.redis.keeper;

import java.io.IOException;
import java.util.Set;

import com.ctrip.xpipe.redis.protocal.Command;
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

	Command slaveConnected(Channel channel);
	
	void slaveDisconntected(Channel channel);
	
	RedisClient clientConnected(Channel channel);
	
	void clientDisConnected(Channel channel);
	
	String getKeeperRunid();
	
	void addCommandsListener(Long offset, CommandsListener listener);

	long readRdbFile(RdbFileListener rdbFileListener) throws IOException;
	
	Set<RedisClient> allClients();
	
	Set<RedisClient> slaves();
}
