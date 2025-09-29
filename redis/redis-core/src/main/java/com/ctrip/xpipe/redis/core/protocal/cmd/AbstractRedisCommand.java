package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
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
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午12:04:13
 */
public abstract class AbstractRedisCommand<T> extends AbstractNettyRequestResponseCommand<T> implements LoggableRedisCommand<T> {

	// consider TCP-retransmit 200ms + 400ms
	public static int DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = Integer.parseInt(System.getProperty("DEFAULT_REDIS_COMMAND_TIME_OUT_SECONDS", "660"));

	public static String KEY_PROXYED_REDIS_COMMAND_TIME_OUT_MILLI = "PROXYED_REDIS_COMMAND_TIME_OUT_MILLI";

	public static int PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI = Integer.parseInt(System.getProperty(KEY_PROXYED_REDIS_COMMAND_TIME_OUT_MILLI, "5000"));

	private int commandTimeoutMilli = DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

	private boolean logResponse = true;

	private boolean logRequest = true;

	protected InOutPayloadFactory inOutPayloadFactory;

	protected RedisProtocolParser redisProtocolParser;

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

	private RedisClientProtocol<?> redisClientProtocol;

	@Override
	protected void doReset() {
		if (redisProtocolParser != null) {
			redisProtocolParser.reset();
		}
	}

	protected String[] splitSpace(String buff) {

		return buff.split("\\s+");
	}

	@Override
	protected T doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {

		if (redisProtocolParser == null) {
			// Lazily initialize the parser, passing along the payload factory
			redisProtocolParser = new RedisProtocolParser(inOutPayloadFactory);
		}

		Object payload = redisProtocolParser.parse(byteBuf);

		if(payload != null) {
			// A complete response has been received
			if(payload instanceof Exception) {
				handleRedisException((Exception)payload);
			}
			return format(payload);
		}

		// Response is not complete yet, need more data
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

	protected Map<String, String> payloadToMap(Object[] payloads) {
		Map<String, String> result = Maps.newHashMap();
		for (int i = 0; i < payloads.length; i += 2) {
			result.put(payloadToString(payloads[i]), payloadToString(payloads[i + 1]));
		}
		return result;
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

	protected AbstractRedisCommand setInOutPayloadFactory(InOutPayloadFactory inOutPayloadFactory) {
		this.inOutPayloadFactory = inOutPayloadFactory;
		return this;
	}
}
