package com.ctrip.xpipe.redis.protocal.protocal;

import java.io.IOException;
import java.io.InputStream;

import com.ctrip.xpipe.redis.protocal.RedisClietProtocol;
import com.ctrip.xpipe.redis.protocal.error.RedisError;


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
	public RedisClietProtocol<RedisError> parse(InputStream ins) throws IOException {
		
		String error = readTilCRLFAsString(ins);
		return new ErrorParser(new RedisError(error));
	}

	@Override
	protected byte[] getWriteBytes() {
		throw new UnsupportedOperationException();
	}


}
