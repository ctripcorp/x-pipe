package com.ctrip.xpipe.redis.protocal.data;

import java.io.IOException;
import java.io.InputStream;

import com.ctrip.xpipe.redis.protocal.RedisClietProtocol;

/**
 * @author wenchao.meng
 *
 * 2016年3月30日 上午11:03:38
 */
public class RequestString extends AbstractRedisClientProtocol<String[]>{
	
	public RequestString(String ...payload) {
		super(payload, true, true);
	}

	public RequestString(boolean logRead, boolean logWrite, String ...payload) {
		super(payload, logRead, logWrite);
	}

	@Override
	public RedisClietProtocol<String[]> parse(InputStream ins) throws IOException {
		return new RequestString(readTilCRLFAsString(ins).split("\\s+"));
	}

	@Override
	protected byte[] getWriteBytes() {
		
		return getRequestBytes(payload);
	}


}
