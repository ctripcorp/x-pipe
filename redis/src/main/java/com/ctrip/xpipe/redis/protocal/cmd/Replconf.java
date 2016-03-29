package com.ctrip.xpipe.redis.protocal.cmd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.AbstractRedisCommand;
import com.ctrip.xpipe.redis.protocal.RedisClietProtocol;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午2:51:47
 */
public class Replconf extends AbstractRedisCommand{
	
	private ReplConfType replConfType;
	private String argu;
	
	public Replconf(ReplConfType replConfType, String argu, OutputStream ous, InputStream ins) {
		super(ous, ins);
		this.replConfType = replConfType;
		this.argu = argu;
	}

	@Override
	public String getName() {
		return "replconf";
	}

	@Override
	protected void handleRedisResponse(RedisClietProtocol<?> redisClietProtocol) throws IOException {
		
		switch(replConfType){
			case LISTENING_PORT:
			case CAPA:
				break;
			case ACK:
				throw new RedisRuntimeException("should not come here, replconf ack has no response!");
			default:
				throw new IllegalStateException("unkonwn repconf type:" + replConfType);
		}
	}
	
	@Override
	protected boolean hasResponse() {
		
		if(replConfType == ReplConfType.ACK){
			return false;
		}
		return true;
	}

	@Override
	protected void doRequest() throws IOException {
		
		writeAndFlush(getName(), replConfType.toString(), argu);
		
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
