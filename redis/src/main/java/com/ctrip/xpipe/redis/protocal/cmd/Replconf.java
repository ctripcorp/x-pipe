package com.ctrip.xpipe.redis.protocal.cmd;


import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;

import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午2:51:47
 */
public class Replconf extends AbstractRedisCommand{
	
	private ReplConfType replConfType;
	private String argu;
	
	public Replconf(ReplConfType replConfType, String argu, Channel channel) {
		super(channel);
		this.replConfType = replConfType;
		this.argu = argu;
	}

	@Override
	public String getName() {
		return "replconf";
	}

	@Override
	protected RESPONSE_STATE handleRedisResponse(RedisClientProtocol<?> redisClietProtocol){
		
		switch(replConfType){
			case LISTENING_PORT:
			case CAPA:
				break;
			case ACK:
				throw new RedisRuntimeException("should not come here, replconf ack has no response!");
			default:
				throw new IllegalStateException("unkonwn repconf type:" + replConfType);
		}
		return RESPONSE_STATE.SUCCESS;
	}
	
	@Override
	protected boolean hasResponse() {
		
		if(replConfType == ReplConfType.ACK){
			return false;
		}
		return true;
	}

	@Override
	protected void doRequest(){
		
		
		boolean logRead = true, logWrite = true;
		
		if(replConfType == ReplConfType.ACK){
			logWrite = false;
		}
		
		RequestStringParser request = new RequestStringParser(logRead, logWrite, getName(), replConfType.toString(), argu);
		writeAndFlush(request.format());
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
}
