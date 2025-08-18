package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperReplDelayConfig;
import com.ctrip.xpipe.redis.keeper.config.RedisReplDelayConfig;
import com.ctrip.xpipe.redis.keeper.config.ReplDelayConfigCache;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
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

	private String idc = null;

	private String region = null;
	
	private AtomicBoolean isKeeper = new AtomicBoolean(false);

	private CLIENT_ROLE clientRole = CLIENT_ROLE.NORMAL;

	private String clientIpAddress;

	private Endpoint endpoint;

	private ReplDelayConfigCache replDelayConfigCache;

	private final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

	private final String CURRENT_REGION = FoundationService.DEFAULT.getRegion();

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
		this.slaveListeningPort = port;
		if(logger.isInfoEnabled()){
			logger.info("[setSlaveListeningPort]" + this + "," + port);
		}
	}

	@Override
	public void setIdc(String _idc) {
		if(logger.isInfoEnabled()){
			logger.info("[setIdc][{}] {}", this, _idc);
		}
		this.idc = _idc;
	}

	@Override
	public String getIdc() {
		return this.idc;
	}

	@Override
	public void setRegion(String region) {
		if (logger.isInfoEnabled()){
			logger.info("[setRegion][{}] {}", this, region);
		}
		this.region = region;
	}

	@Override
	public String getRegion() {
		return this.region;
	}

	public boolean isCrossRegion() {
		return !StringUtil.isEmpty(this.region) && !CURRENT_REGION.equalsIgnoreCase(this.region);
	}

	public void setReplDelayConfigCache(ReplDelayConfigCache replDelayConfigCache) {
		this.replDelayConfigCache = replDelayConfigCache;
	}

	@Override
	public long getDelayMilli() {
		if (null == replDelayConfigCache || !isKeeper() || StringUtil.isEmpty(idc)) return super.getDelayMilli();
		KeeperReplDelayConfig replDelayConfig = replDelayConfigCache.getKeeperReplDelayConfig(idc);
		if (null == replDelayConfig) return super.getDelayMilli();
		else return replDelayConfig.getDelayMilli();
	}

	public int getLimitBytesPerSecond() {
		if (null == replDelayConfigCache) return super.getLimitBytesPerSecond();
		if (isKeeper() && isCrossRegion()) {
			return replDelayConfigCache.getCrossRegionBytesLimit();
		} else if (!isKeeper()) {
			RedisReplDelayConfig replDelayConfig = replDelayConfigCache.getRedisReplDelayConfig();
			if (null == replDelayConfig) return super.getLimitBytesPerSecond();
			else return replDelayConfig.getBytesLimitPerSecond();
		} else {
			return super.getLimitBytesPerSecond();
		}
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
	public GapAllowRedisSlave becomeGapAllowRedisSlave() {
		if (clientRole != CLIENT_ROLE.NORMAL) {
			logger.info("[becomeGapAllowRedisSlave][already slave] {}", this);
			return null;
		} else {
			this.clientRole = CLIENT_ROLE.SLAVE;
			GapAllowRedisSlave redisSlave = new GapAllowRedisSlave(this);
			notifyObservers(redisSlave);
			return redisSlave;
		}
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
