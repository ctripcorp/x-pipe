package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.netty.commands.AbstractNettyRequestResponseCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.DirectByteBufInStringOutPayload;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.LoggableRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.*;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午12:04:13
 */
public abstract class AbstractRedisCommand<T> extends AbstractNettyRequestResponseCommand<T> implements LoggableRedisCommand<T> {

	// consider TCP-retransmit 200ms + 400ms
	public static int DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = Integer.parseInt(System.getProperty("DEFAULT_REDIS_COMMAND_TIME_OUT_SECONDS", "660"));

	public static int PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI = Integer.parseInt(System.getProperty("PROXYED_REDIS_COMMAND_TIME_OUT_SECONDS", "5000"));

	private int commandTimeoutMilli = DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

	private boolean logResponse = true;

	private boolean logRequest = true;

	protected InOutPayloadFactory inOutPayloadFactory;

	public AbstractRedisCommand(String host, int port, ScheduledExecutorService scheduled){
		super(host, port, scheduled);
	}

	public AbstractRedisCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}

	public AbstractRedisCommand(String host, int port, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
		super(host, port, scheduled);
		this.commandTimeoutMilli = commandTimeoutMilli;
	}

	public AbstractRedisCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
		super(clientPool, scheduled);
		this.commandTimeoutMilli = commandTimeoutMilli;
	}

	public static enum COMMAND_RESPONSE_STATE{
		READING_SIGN,
		READING_CONTENT;
	}
	
	protected COMMAND_RESPONSE_STATE commandResponseState = COMMAND_RESPONSE_STATE.READING_SIGN;
	
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
						redisClientProtocol = new ArrayParser().setInOutPayloadFactory(inOutPayloadFactory);
						break;
					case RedisClientProtocol.DOLLAR_BYTE:
						if(inOutPayloadFactory != null) {
							redisClientProtocol = new CommandBulkStringParaser(inOutPayloadFactory.create());
						} else {
							redisClientProtocol = new CommandBulkStringParaser(getBulkStringPayload());
						}
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
						handleRedisException((Exception)payload);
					}
					return format(payload);
				}
				break;
			default:
				break;
		}
		return null;
	}
	
	protected void handleRedisException(Exception redisException) throws Exception {
		throw redisException;
	}

	protected abstract T format(Object payload);
	
	
	protected InOutPayload getBulkStringPayload() {
		return new ByteArrayOutputStreamPayload();
	}
	
	
	protected String payloadToString(Object payload) {

		if(payload instanceof String){
			
			getLogger().debug("[payloadToString]{}", payload);
			return (String)payload;
		}
		if(payload instanceof ByteArrayOutputStreamPayload){
			
			ByteArrayOutputStreamPayload baous = (ByteArrayOutputStreamPayload) payload;
			String result = new String(baous.getBytes(), Codec.defaultCharset); 
			getLogger().debug("[payloadToString]{}", result);
			return result;
		}
		if(payload instanceof DirectByteBufInStringOutPayload) {
			return payload.toString();
		}

		String clazz = payload == null ? "null" : payload.getClass().getSimpleName();
		throw new IllegalStateException(String.format("unknown payload %s:%s", clazz, StringUtil.toString(payload)));
	}
	
	
	protected Integer payloadToInteger(Object payload) {
		
		if(payload instanceof Integer){
			return (Integer) payload;
		}
		
		String result = payloadToString(payload);
		return Integer.parseInt(result);
	}

	protected Boolean payloadToBoolean(Object payload) {
		
		if(payload instanceof Boolean){
			return (Boolean) payload;
		}
		
		String result = payloadToString(payload);
		return Boolean.parseBoolean(result);
	}

	protected String[] payloadToStringArray(Object payload) {
		if(!(payload instanceof Object[])) {
			throw new RedisRuntimeException(String.format("payload not array: %s", payload));
		}
		Object[] objects = (Object[]) payload;
		String[] result = new String[objects.length];

		for(int i = 0; i < result.length; i++) {
			result[i] = payloadToString(objects[i]);
		}

		return result;
	}

	protected Long payloadToLong(Object payload) {

		if(payload instanceof Long){
			return (Long) payload;
		}

		String result = payloadToString(payload);
		return Long.parseLong(result);
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}
	
	@Override
	public int getCommandTimeoutMilli() {
		return commandTimeoutMilli;
	}
	
	public void setCommandTimeoutMilli(int commandTimeoutMilli) {
		this.commandTimeoutMilli = commandTimeoutMilli;
	}

	@Override
	protected boolean logRequest() {
		return logRequest;
	}

	@Override
	protected boolean logResponse() {
		return logResponse;
	}

	@Override
	public void logResponse(boolean logResponse) {
		this.logResponse = logResponse;
	}

	@Override
	public void logRequest(boolean logRequest) {
		this.logRequest = logRequest;
	}

	protected boolean isProxyEnabled(Endpoint endpoint) {
		return endpoint instanceof ProxyEnabled;
	}

	protected AbstractRedisCommand setInOutPayloadFactory(InOutPayloadFactory inOutPayloadFactory) {
		this.inOutPayloadFactory = inOutPayloadFactory;
		return this;
	}
}
