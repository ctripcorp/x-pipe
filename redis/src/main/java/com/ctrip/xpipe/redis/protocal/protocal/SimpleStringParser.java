package com.ctrip.xpipe.redis.protocal.protocal;

import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;

import io.netty.buffer.ByteBuf;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:31:56
 */
public class SimpleStringParser extends AbstractRedisClientProtocol<String>{

	public SimpleStringParser() {
	}
	
	public SimpleStringParser(String payload) {
		super(payload, true, true);
	}
	
	public SimpleStringParser(String payload, boolean logRead, boolean logWrite) {
		super(payload, logRead, logWrite);
	}
	
	@Override
	public RedisClientProtocol<String> read(ByteBuf byteBuf){
		
		String data = readTilCRLFAsString(byteBuf);
		if(data == null){
			return null;
		}
		return new SimpleStringParser(data);
	}

	@Override
	protected byte[] getWriteBytes() {
		
		return getRequestBytes(PLUS_BYTE, payload);
	}


}
