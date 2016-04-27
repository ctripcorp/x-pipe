package com.ctrip.xpipe.redis.protocal.protocal;

import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.error.RedisError;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:17:45
 */
public class RedisErrorParser extends AbstractRedisClientProtocol<RedisError>{
	
	public RedisErrorParser(){
	}
	
	public RedisErrorParser(String errorMessage) {
		this(new RedisError(errorMessage));
	}
	

	public RedisErrorParser(RedisError redisError) {
		super(redisError, true, true);
	}

	@Override
	public RedisClientProtocol<RedisError> read(ByteBuf byteBuf){
		
		String error = readTilCRLFAsString(byteBuf);
		return new RedisErrorParser(new RedisError(error));
	}

	
	@Override
	protected ByteBuf getWriteByteBuf() {
		
		return Unpooled.wrappedBuffer(getRequestBytes(MINUS_BYTE, payload.errorMessage()));
	}

}
