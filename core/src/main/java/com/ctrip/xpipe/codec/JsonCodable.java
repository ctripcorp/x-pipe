package com.ctrip.xpipe.codec;

import com.ctrip.xpipe.api.codec.Codec;

/**
 * @author wenchao.meng
 *
 * Jul 27, 2016
 */
public abstract class JsonCodable {
	
	private static Codec codec = new JsonCodec();

	
	public byte[] encode(){
		return codec.encodeAsBytes(this);
	}
	
	public static <V> V decode(byte []bytes, Class<V> clazz){
		
		return codec.decode(bytes, clazz);
		
	} 

}
