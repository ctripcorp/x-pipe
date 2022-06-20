package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.Closeable;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:25:07
 */
public interface RedisClient<T extends RedisServer> extends Observable, Infoable, Closeable, RedisRole, Releasable, Keeperable{
	
	public static enum CLIENT_ROLE{
		NORMAL,
		SLAVE
	}
	
	RedisSlave becomeSlave();

	// for xsync
	RedisSlave becomeXSlave();
	
	T getRedisServer();

	void setSlaveListeningPort(int port);

	int getSlaveListeningPort();

	void setClientIpAddress(String host);

	String getClientIpAddress();

	void capa(CAPA capa);

	boolean capaOf(CAPA capa);

	Set<CAPA> getCapas();
	
	String []readCommands(ByteBuf byteBuf);

	String info();

	String ip();
	
	Channel channel();

	void sendMessage(ByteBuf byteBuf);
	
	void sendMessage(byte[] bytes);
	
	void addChannelCloseReleaseResources(Releasable releasable);

	void setClientEndpoint(Endpoint endpoint);

	Endpoint getClientEndpoint();
}
