package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.api.codec.Codec;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class ByteBufUtils {
	
	public static byte[] readToBytes(ByteBuf byteBuf){
		
		byte []result = new byte[byteBuf.readableBytes()];
		byteBuf.readBytes(result);
		//TODO not copy
		return result;
	}

	public static String readToString(ByteBuf byteBuf){
		
		byte []result = readToBytes(byteBuf);
		return new String(result,Codec.defaultCharset);
			
	}

}
