package com.ctrip.xpipe.redis.core.protocal;


import io.netty.buffer.ByteBuf;

import java.io.IOException;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:27:48
 */
public interface RedisClientProtocol<T> extends RedisProtocol{
	
	public static final byte DOLLAR_BYTE = '$';
	public static final byte ASTERISK_BYTE = '*';
	public static final byte PLUS_BYTE = '+';
	public static final byte MINUS_BYTE = '-';
	public static final byte COLON_BYTE = ':';
	
	public static byte [] EOF = "EOF:".getBytes();

	
	/**
	 * 转化成功，返回结果；如果数据不足，返回null，等待数据继续读取
	 * @param byteBuf
	 * @return
	 * @throws IOException 
	 */
	RedisClientProtocol<T> read(ByteBuf byteBuf);

	ByteBuf format();
	
	T getPayload();
	
	boolean supportes(Class<?> clazz);

	void reset();
}
