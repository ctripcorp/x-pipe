package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.IpUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:30:49
 */
public class DefaultRedisClient extends AbstractRedisClient<RedisKeeperServer> implements RedisClient<RedisKeeperServer> {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private Set<CAPA>  capas = new HashSet<CAPA>(); 

	private int slaveListeningPort;
	
	private AtomicBoolean isKeeper = new AtomicBoolean(false);

	private CLIENT_ROLE clientRole = CLIENT_ROLE.NORMAL;

	private String clientIpAddress;

	private Endpoint endpoint;

	public DefaultRedisClient(Channel channel, RedisKeeperServer redisKeeperServer) {
		super(channel, redisKeeperServer);
		String remoteIpLocalPort = ChannelUtil.getRemoteAddr(channel);
		channel.closeFuture().addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				logger.info("[operationComplete][channel closed]{}, {}, {}, {}", future.channel(), this, future.isDone(), future.isSuccess());
				if (!future.isSuccess()) logger.info("[operationComplete]", future.cause());
				release();
			}
		});
	}

	@Override
	public void setSlaveListeningPort(int port) {
		if(logger.isInfoEnabled()){
			logger.info("[setSlaveListeningPort]" + this + "," + port);
		}
		this.slaveListeningPort = port;
	}

	@Override
	public void capa(CAPA capa) {
		logger.info("[capa]{}, {}", capa, this);
		capas.add(capa);
	}
	
	@Override
	public boolean capaOf(CAPA capa) {
		return capas.contains(capa);
	}
	
	@Override
	public int getSlaveListeningPort() {
		return this.slaveListeningPort;
	}

	@Override
	public void setClientIpAddress(String host) {
		this.clientIpAddress = host;
	}

	@Override
	public String getClientIpAddress() {
		return this.clientIpAddress;
	}

	@Override
	public String ip() {
		if(this.clientIpAddress != null) {
			return clientIpAddress;
		}
		Channel channel = channel();
		return channel == null? "null": IpUtils.getIp(channel.remoteAddress());
	}
	
	@Override
	public RedisSlave becomeSlave() {
		
		RedisSlave redisSlave = null;
		switch(clientRole){
			case NORMAL:
				logger.info("[becomeSlave]" + this);
				redisSlave = new DefaultRedisSlave(this); 
				notifyObservers(redisSlave);
				break;
			case SLAVE:
				logger.info("[becomeSlave][already slave]" + this);
				break;
			default:
				throw new IllegalStateException("unknown state:" + clientRole);
		}
		return redisSlave;
	}

	@Override
	public RedisSlave becomeXSlave() {
		RedisSlave redisSlave = null;
		switch(clientRole){
			case NORMAL:
				logger.info("[becomeXSlave]" + this);
				redisSlave = new XsyncRedisSlave(this);
				notifyObservers(redisSlave);
				break;
			case SLAVE:
				logger.info("[becomeXSlave][already slave]" + this);
				break;
			default:
				throw new IllegalStateException("unknown state:" + clientRole);
		}
		return redisSlave;
	}

	@Override
	public void setClientEndpoint(Endpoint endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public Endpoint getClientEndpoint() {
		return this.endpoint;
	}

	@Override
	public Set<CAPA> getCapas() {
		return new HashSet<>(capas);
	}

	@Override
	public boolean isKeeper() {
		return isKeeper.get();
	}

	@Override
	public void setKeeper() {
		isKeeper.set(true);
		logger.info("[setKeeper]{}", this);
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}
}
