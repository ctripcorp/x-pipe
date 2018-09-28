package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.api.codec.Codec;

import java.nio.ByteBuffer;

/**
 * @author wenchao.meng
 *
 *         Aug 26, 2016
 */
public class ByteBufferUtils {

	public static byte[] readToBytes(ByteBuffer byteBuffer) {

		int remain = byteBuffer.remaining();
		byte[] data = new byte[remain];
		byteBuffer.get(data);
		return data;
	}

	public static String readToString(ByteBuffer byteBuffer) {

		byte[] result = readToBytes(byteBuffer);
		return new String(result, Codec.defaultCharset);

	}

}
