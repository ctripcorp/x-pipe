package com.ctrip.xpipe.redis.core.protocal.cmd;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.AbstractNettyRequestResponseCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.LongParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午12:04:13
 */
public abstract class AbstractRedisCommand<T> extends AbstractNettyRequestResponseCommand<T> {

	public AbstractRedisCommand(String host, int port){
		super(host, port);
	}

	public AbstractRedisCommand(SimpleObjectPool<NettyClient> clientPool) {
		super(clientPool);
	}

	public static enum COMMAND_RESPONSE_STATE{
		READING_SIGN,
		READING_CONTENT;
	}
	
	private COMMAND_RESPONSE_STATE commandResponseState = COMMAND_RESPONSE_STATE.READING_SIGN;
	
	private int sign;
	
	private RedisClientProtocol<?> redisClientProtocol;

	
	@Override
	protected void doReset() {
		commandResponseState = COMMAND_RESPONSE_STATE.READING_SIGN;
	}
	
	
	protected String[] splitSpace(String buff) {
		
		return buff.split("\\s+");
	}
	
	@Override
	protected T doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {
		
		switch(commandResponseState){
		
		case READING_SIGN:
			int readable = byteBuf.readableBytes();
			for(int i = 0; i < readable ; i++){
				
				sign = byteBuf.readByte();
				switch(sign){
					case '\r':
						break;
					case '\n':
						break;
					case RedisClientProtocol.MINUS_BYTE:
						redisClientProtocol = new RedisErrorParser();
						break;
					case RedisClientProtocol.ASTERISK_BYTE:
						redisClientProtocol = new ArrayParser();
						break;
					case RedisClientProtocol.DOLLAR_BYTE:
						redisClientProtocol = new BulkStringParser(getBulkStringPayload());
						break;
					case RedisClientProtocol.COLON_BYTE:
						redisClientProtocol = new LongParser();
						break;
					case RedisClientProtocol.PLUS_BYTE:
						redisClientProtocol = new SimpleStringParser();
						break;
					default:
						throw new RedisRuntimeException("unkonwn sign:" + (char)sign);
				}
				
				if(redisClientProtocol != null){
					commandResponseState = COMMAND_RESPONSE_STATE.READING_CONTENT;
					break;
				}
			}
			
			if(redisClientProtocol == null){
				break;
			}
			case READING_CONTENT:
				RedisClientProtocol<?> result = redisClientProtocol.read(byteBuf);
				if(result != null){
					Object payload = result.getPayload();
					if(payload instanceof Exception){
						throw (Exception)payload;
					}
					return format(payload);
				}
				break;
			default:
				break;
		}
		return null;
	}
	
	protected abstract T format(Object payload);
	
	
	protected InOutPayload getBulkStringPayload() {
		return new ByteArrayOutputStreamPayload();
	}
	
	
	protected String payloadToString(Object payload) {

		if(payload instanceof String){
			
			logger.debug("[payloadToString]{}", payload);
			return (String)payload;
		}if(payload instanceof ByteArrayOutputStreamPayload){
			
			ByteArrayOutputStreamPayload baous = (ByteArrayOutputStreamPayload) payload;
			String result = new String(baous.getBytes(), Codec.defaultCharset); 
			logger.debug("[payloadToString]{}", result);
			return result;
		}
		
		throw new IllegalStateException("unknown payload:" + payload);
	}

}
