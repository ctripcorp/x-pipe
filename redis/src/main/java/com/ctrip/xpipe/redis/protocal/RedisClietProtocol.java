package com.ctrip.xpipe.redis.protocal;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:27:48
 */
public interface RedisClietProtocol<T> extends RedisProtocol{
	
	public static final byte DOLLAR_BYTE = '$';
	public static final byte ASTERISK_BYTE = '*';
	public static final byte PLUS_BYTE = '+';
	public static final byte MINUS_BYTE = '-';
	public static final byte COLON_BYTE = ':';

	
	
	/**
	 * 转化成功，返回结果；如果数据不足，返回null，等待数据继续读取
	 * @param byteBuf
	 * @return
	 * @throws IOException 
	 */
	T parse(InputStream ins) throws IOException;
	
	void write(OutputStream ous) throws IOException;
}
