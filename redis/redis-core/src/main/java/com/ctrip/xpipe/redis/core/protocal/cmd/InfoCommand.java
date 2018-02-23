package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         May 9, 2016 5:42:01 PM
 */
public class InfoCommand extends AbstractRedisCommand<String> {

	private String args;

	public InfoCommand(SimpleObjectPool<NettyClient> clientPool, String args, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		this.args = args;
	}

	public InfoCommand(SimpleObjectPool<NettyClient> clientPool, INFO_TYPE infoType, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		this.args = infoType.cmd();
	}

	@Override
	public String getName() {
		return "info";
	}

	@Override
	public ByteBuf getRequest() {
		
		RequestStringParser requestString = new RequestStringParser(getName(), args);
		return requestString.format();
	}

	@Override
	protected String format(Object payload) {

		return payloadToString(payload);
	}
	
	@Override
	public String toString() {
		return getName() + " " + (args == null? "":args);
	}


	public static enum INFO_TYPE{

		REPLICATION,
		SERVER,
		SENTINEL;

		public String cmd(){
			return toString().toLowerCase();
		}
	}

}
