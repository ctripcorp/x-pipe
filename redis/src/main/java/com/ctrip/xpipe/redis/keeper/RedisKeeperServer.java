package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.PsyncObserver;

import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:09:23
 */
public interface RedisKeeperServer extends RedisServer, PsyncObserver{
	
	long getReploffset();
	
	Command slaveConnected(Channel channel);
	
	void slaveDisconntected(Channel channel);
	
	void clientConnected(Channel channel);
	
	void clientDisConnected(Channel channel);
}
