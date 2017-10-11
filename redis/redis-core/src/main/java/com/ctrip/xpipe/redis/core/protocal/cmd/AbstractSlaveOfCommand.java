package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         May 9, 2016 3:19:37 PM
 */
public abstract class AbstractSlaveOfCommand extends AbstractRedisCommand<String> {
	
	protected String ip;
	protected int port;
	private String param = "";

	public AbstractSlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}
	
	public AbstractSlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port, ScheduledExecutorService scheduled){
		this(clientPool, ip, port, "", scheduled);
	}

	public AbstractSlaveOfCommand(SimpleObjectPool<NettyClient> clientPool, String ip, int port, String param, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		this.ip = ip;
		this.port = port;
		this.param = param;
	}

	@Override
	public ByteBuf getRequest() {
		
		RequestStringParser requestString = null;
		if(ip == null){
			requestString = new RequestStringParser(getName(), "no", "one", param);
		}else{
			requestString = new RequestStringParser(getName(), ip, String.valueOf(port), param);
		}
		return requestString.format();
	}

	@Override
	public String toString() {

		String target = getClientPool() == null? "null" : getClientPool().desc();

		if(StringUtil.isEmpty(ip)){
			return String.format("%s: %s no one", target, getName());
		}else{
			return String.format("%s: %s %s %d %s", target, getName(), ip, port, param);
		}
	}

	@Override
	protected String format(Object payload) {
		return payloadToString(payload);
	}
}
