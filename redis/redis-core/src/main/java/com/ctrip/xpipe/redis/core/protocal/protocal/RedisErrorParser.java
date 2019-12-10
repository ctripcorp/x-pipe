package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:17:45
 */
public class RedisErrorParser extends AbstractRedisClientProtocol<RedisError>{

	private static final Logger logger = LoggerFactory.getLogger(RedisErrorParser.class);
	
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

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public boolean supportes(Class<?> clazz) {
		return RedisError.class.isAssignableFrom(clazz);
	}

}
