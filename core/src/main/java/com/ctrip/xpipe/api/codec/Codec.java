package com.ctrip.xpipe.api.codec;

import java.nio.charset.Charset;

import com.ctrip.xpipe.codec.JsonCodec;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:29:12
 */
public interface Codec {

	public static final Charset defaultCharset = Charset.forName("UTF-8");
	
	public static final Codec DEFAULT = new JsonCodec();
	
	String encode(Object obj);

	byte[] encodeAsBytes(Object obj);

	<T> T decode(String data, Class<T> clazz);
	
	<T> T decode(byte[] data, Class<T> clazz);
	
	<T> T decode(String data, GenericTypeReference<T> reference);
	
	<T> T decode(byte []data, GenericTypeReference<T> reference);
	
}
