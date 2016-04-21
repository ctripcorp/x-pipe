package com.ctrip.xpipe.redis.protocal.protocal;

import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.error.RedisError;

import io.netty.buffer.ByteBuf;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:17:45
 */
public class ErrorParser extends AbstractRedisClientProtocol<RedisError>{
	
	public ErrorParser() {
	}
	

	public ErrorParser(RedisError redisError) {
		super(redisError, true, true);
	}

	@Override
	public RedisClientProtocol<RedisError> read(ByteBuf byteBuf){
		
		String error = readTilCRLFAsString(byteBuf);
		return new ErrorParser(new RedisError(error));
	}

	@Override
	protected byte[] getWriteBytes() {
		throw new UnsupportedOperationException();
	}


}
