package com.ctrip.xpipe.payload;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * @author wenchao.meng
 *
 *         2016年4月27日 上午9:37:11
 */
public class ByteArrayWritableByteChannelTest extends AbstractTest {
	
	@Test
	public void testInfo(){
	}

	@Test
	public void testWrite() throws IOException {

		String content = randomString();

		try (ByteArrayWritableByteChannel channel = new ByteArrayWritableByteChannel()) {

			ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes()); 
			channel.write(byteBuffer);

			byte[] result = channel.getResult();

			Assert.assertEquals(0, byteBuffer.remaining());
			Assert.assertEquals(content, new String(result));
		}
	}

}
