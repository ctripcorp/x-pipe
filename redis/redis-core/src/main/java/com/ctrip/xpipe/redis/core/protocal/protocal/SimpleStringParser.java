package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:31:56
 */
public class SimpleStringParser extends AbstractRedisClientProtocol<String>{
	
	public static final byte[] OK = "+OK\r\n".getBytes();

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
		
		int beginIndex = 0;
		int endIndex = data.length();
		int dataLength = data.length();
		if(data.charAt(0) == PLUS_BYTE){
			beginIndex = 1;
		}
		
		if(data.charAt(dataLength - 2) == '\r' && data.charAt(dataLength - 1) == '\n'){
			endIndex -= 2;
		}
		return new SimpleStringParser(data.substring(beginIndex, endIndex));
	}

	
	@Override
	protected ByteBuf getWriteByteBuf() {
		
		return Unpooled.wrappedBuffer(getRequestBytes(PLUS_BYTE, payload));
	}

	@Override
	public boolean supportes(Class<?> clazz) {
		return String.class.isAssignableFrom(clazz);
	}

}
