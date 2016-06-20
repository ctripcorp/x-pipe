package com.ctrip.xpipe.redis.keeper.protocal.protocal;


import com.ctrip.xpipe.redis.keeper.protocal.RedisClientProtocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author wenchao.meng
 *
 * 2016年3月30日 上午11:03:38
 */
public class RequestStringParser extends AbstractRedisClientProtocol<String[]>{
	
	public RequestStringParser(String ...payload) {
		super(payload, true, true);
	}

	public RequestStringParser(boolean logRead, boolean logWrite, String ...payload) {
		super(payload, logRead, logWrite);
	}

	@Override
	public RedisClientProtocol<String[]> read(ByteBuf byteBuf){
		String data = readTilCRLFAsString(byteBuf);
		if(data == null){
			return null;
		}
		
		return new RequestStringParser(data.split("\\s+"));
	}

	@Override
	protected ByteBuf getWriteByteBuf() {
		
		return Unpooled.wrappedBuffer(getRequestBytes(payload));
	}
}
