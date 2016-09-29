package com.ctrip.xpipe.netty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.ctrip.xpipe.api.codec.Codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class ByteBufUtils {
	
	public static byte[] readToBytes(ByteBuf byteBuf){

		
		if(byteBuf instanceof CompositeByteBuf){
			CompositeByteBuf compositeByteBuf = (CompositeByteBuf) byteBuf;
			ByteArrayOutputStream baous = new ByteArrayOutputStream();
			for(ByteBuf single : compositeByteBuf){
				try {
					baous.write(readToBytes(single));
				} catch (IOException e) {
					throw new IllegalStateException("write to ByteArrayOutputStream error", e);
				}
			}
			return baous.toByteArray();
		}else{
			byte []result = new byte[byteBuf.readableBytes()];
			byteBuf.readBytes(result);
			return result;
		}
	}

	public static String readToString(ByteBuf byteBuf){
		
		byte []result = readToBytes(byteBuf);
		return new String(result,Codec.defaultCharset);
			
	}

}
