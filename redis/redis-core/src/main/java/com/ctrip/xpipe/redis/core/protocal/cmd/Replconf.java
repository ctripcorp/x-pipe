package com.ctrip.xpipe.redis.core.protocal.cmd;


import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午2:51:47
 */
public class Replconf extends AbstractRedisCommand<Object>{
	
	private ReplConfType replConfType;
	private String argu;
	
	public Replconf(SimpleObjectPool<NettyClient> clientPool, ReplConfType replConfType, String argu) {
		super(clientPool);
		this.replConfType = replConfType;
		this.argu = argu;
	}

	@Override
	public String getName() {
		return "replconf";
	}

	@Override
	protected boolean hasResponse() {
		
		if(replConfType == ReplConfType.ACK){
			return false;
		}
		return true;
	}

	public enum ReplConfType{
		
		LISTENING_PORT("listening-port"),
		CAPA("capa"),
		ACK("ack");
		
		
		private String command;
		ReplConfType(String command){
			this.command = command;
		}

		@Override
		public String toString() {
			return command;
		}
	}

	@Override
	protected ByteBuf getRequest() {
		
		boolean logRead = true, logWrite = true;
		
		if(replConfType == ReplConfType.ACK){
			logWrite = false;
		}
		
		RequestStringParser request = new RequestStringParser(logRead, logWrite, getName(), replConfType.toString(), argu);
		return request.format();
	}
	
	@Override
	protected boolean logRequest() {
		if(replConfType == ReplConfType.ACK){
			return false;
		}
		return true;

	}
	
	@Override
	protected boolean logResponse() {
		return logRequest();
	}
	
	

	@Override
	protected Object format(Object payload) {
		return payload;
	}
}
