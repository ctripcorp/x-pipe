package com.ctrip.xpipe.redis.protocal.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ctrip.xpipe.redis.protocal.error.RedisError;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:17:45
 */
public class ErrorParser extends AbstractRedisClientProtocol<RedisError>{
	
	

	@Override
	public RedisError parse(InputStream ins) throws IOException {
		
		
		String error = readTilCRLFAsString(ins);
		return new RedisError(error);
	}

	@Override
	protected void doWrite(OutputStream ous) throws IOException {
		throw new UnsupportedOperationException();
	}


}
