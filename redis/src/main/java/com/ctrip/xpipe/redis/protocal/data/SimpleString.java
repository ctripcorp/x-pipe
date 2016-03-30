package com.ctrip.xpipe.redis.protocal.data;

import java.io.IOException;
import java.io.InputStream;

import com.ctrip.xpipe.redis.protocal.RedisClietProtocol;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:31:56
 */
public class SimpleString extends AbstractRedisClientProtocol<String>{

	public SimpleString() {
	}
	
	public SimpleString(String payload) {
		super(payload, true, true);
	}
	
	public SimpleString(String payload, boolean logRead, boolean logWrite) {
		super(payload, logRead, logWrite);
	}
	
	@Override
	public RedisClietProtocol<String> parse(InputStream ins) throws IOException {
		
		return new SimpleString(readTilCRLFAsString(ins));
	}

	@Override
	protected byte[] getWriteBytes() {
		
		return getRequestBytes(PLUS_BYTE, payload);
	}


}
