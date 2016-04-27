package com.ctrip.xpipe.redis.protocal.protocal;

import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:31:56
 */
public class IntegerParser extends AbstractRedisClientProtocol<Integer>{

	public IntegerParser() {
	}
	
	public IntegerParser(Integer payload) {
		super(payload, true, true);
	}
	
	public IntegerParser(Integer payload, boolean logRead, boolean logWrite) {
		super(payload, logRead, logWrite);
	}
	
	@Override
	public RedisClientProtocol<Integer> read(ByteBuf byteBuf){
		
		String data = readTilCRLFAsString(byteBuf);
		if(data == null){
			return null;
		}
		if(data.charAt(0) != COLON_BYTE){
			throw new RedisRuntimeException("expecte integer format, but:" + data);
		}
		return new IntegerParser(Integer.valueOf(data.substring(1).trim()));
	}

	@Override
	protected ByteBuf getWriteByteBuf() {
		
		return Unpooled.wrappedBuffer(getRequestBytes(COLON_BYTE, payload));
	}
}
