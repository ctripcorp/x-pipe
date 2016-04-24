package com.ctrip.xpipe.redis.protocal.cmd;


import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.protocal.protocal.IntegerParser;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午12:04:13
 */
public abstract class AbstractRedisCommand extends AbstractCommand {
	
	
	private Channel channel;
	
	public static enum COMMAND_RESPONSE_STATE{
		READING_SIGN,
		READING_CONTENT;
	}
	
	private COMMAND_RESPONSE_STATE commandResponseState = COMMAND_RESPONSE_STATE.READING_SIGN;
	
	private int sign;
	
	private RedisClientProtocol<?> redisClientProtocol;
	
	protected AbstractRedisCommand(Channel channel) {
		this.channel = channel;
	}
	protected String[] splitSpace(String buff) {
		
		return buff.split("\\s+");
	}

	@Override
	protected RESPONSE_STATE doHandleResponse(ByteBuf byteBuf) throws XpipeException{

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
							throw new UnsupportedOperationException("array not supported yet!");
						case RedisClientProtocol.DOLLAR_BYTE:
							redisClientProtocol = new BulkStringParser(getBulkStringPayload());
							break;
						case RedisClientProtocol.COLON_BYTE:
							redisClientProtocol = new IntegerParser();
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
					return handleRedisResponse(result);
				}
				break;
			default:
				break;
		}
		
		return RESPONSE_STATE.CONTINUE;
	}
	
	protected InOutPayload getBulkStringPayload() {
		return null;
	}

	protected abstract RESPONSE_STATE handleRedisResponse(RedisClientProtocol<?> redisClientProtocol);



	protected void writeAndFlush(byte[] format) {
		channel.writeAndFlush(format);
	}
}
