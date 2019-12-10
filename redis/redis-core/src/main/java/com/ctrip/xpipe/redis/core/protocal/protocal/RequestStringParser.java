package com.ctrip.xpipe.redis.core.protocal.protocal;


import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * 2016年3月30日 上午11:03:38
 */
public class RequestStringParser extends AbstractRedisClientProtocol<String[]>{

	private static final Logger logger = LoggerFactory.getLogger(RequestStringParser.class);
	
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

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public boolean supportes(Class<?> clazz) {
		return String.class.isAssignableFrom(clazz);
	}
}
