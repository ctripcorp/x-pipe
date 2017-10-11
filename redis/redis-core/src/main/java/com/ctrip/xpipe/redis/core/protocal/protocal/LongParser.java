package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:31:56
 */
public class LongParser extends AbstractRedisClientProtocol<Long>{

	public LongParser() {
	}
	
	public LongParser(Long payload) {
		super(payload, true, true);
	}
	
	public LongParser(Integer payload) {
		super(payload.longValue(), true, true);
	}
	
	public LongParser(Long payload, boolean logRead, boolean logWrite) {
		super(payload, logRead, logWrite);
	}
	
	@Override
	public RedisClientProtocol<Long> read(ByteBuf byteBuf){
		
		String data = readTilCRLFAsString(byteBuf);
		if(data == null){
			return null;
		}
		if(data.charAt(0) != COLON_BYTE){
			throw new RedisRuntimeException("expecte integer format, but:" + data);
		}
		return new LongParser(Long.valueOf(data.substring(1).trim()));
	}

	@Override
	protected ByteBuf getWriteByteBuf() {
		
		return Unpooled.wrappedBuffer(getRequestBytes(COLON_BYTE, payload));
	}

	@Override
	public boolean supportes(Class<?> clazz) {
		return Long.class.isAssignableFrom(clazz) || Integer.class.isAssignableFrom(clazz);
	}
}
