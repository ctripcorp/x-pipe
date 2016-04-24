package com.ctrip.xpipe.payload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午8:58:12
 */
public class ByteArrayOutputStreamPayloadTest extends AbstractTest{
	
	
	@Test
	public void testInout() throws IOException{
		
		ByteArrayOutputStreamPayload payload = new ByteArrayOutputStreamPayload();
		String randomStr = randomString();
		
		ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(randomStr.length());
		byteBuf.writeBytes(randomStr.getBytes());
		payload.startInput();
		payload.in(byteBuf);
		payload.endInput();
		
		
		final ByteBuf result = ByteBufAllocator.DEFAULT.buffer(randomStr.length());
		payload.startOutput();
		long wroteLength = payload.out(new WritableByteChannel() {
			
			@Override
			public boolean isOpen() {
				return false;
			}
			
			@Override
			public void close() throws IOException {
				
			}
			
			@Override
			public int write(ByteBuffer src) throws IOException {
				
				int readable = result.readableBytes();
				result.writeBytes(src);
				return result.readableBytes() - readable;
			}
		});
		payload.endOutput();

		
		Assert.assertEquals(randomStr.length(), wroteLength);
		
		byte []resultArray = new byte[(int) wroteLength];
		result.readBytes(resultArray);
		Assert.assertEquals(randomStr, new String(resultArray));
	}

}
