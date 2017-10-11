package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午9:15:16
 */
public class ArrayParserTest extends AbstractRedisProtocolTest{
	
	
	private ArrayParser arrayParser = new ArrayParser();
	
	@Test
	public void testArray() throws IOException{
		
		String str1 = randomString();
		String str2 = randomString();
		Long   long3 = 1024L;
		String []data = new String[]{
				"*3\r\n",
				"+"+str1+"\r\n",
				"$" + str2.length() + "\r\n" + str2 + "\r\n",
				":" + long3 + "\r\n"
		};
		
		ArrayParser resultParser = (ArrayParser) parse(arrayParser, data);
		
		Object[] result = resultParser.getPayload();
		
		Assert.assertEquals(3, result.length);
		
		Assert.assertEquals(str1, result[0]);
		
		ByteArrayOutputStreamPayload bap = (ByteArrayOutputStreamPayload) result[1];
		ByteArrayWritableByteChannel channel = new ByteArrayWritableByteChannel();
		bap.out(channel);
		Assert.assertEquals(str2, new String(channel.getResult()));
		
		Assert.assertEquals(long3, result[2]);
	}

	@Test
	public void testFormat(){

		ArrayParser arrayParser = new ArrayParser(new Object[]{1, "123"});
		ByteBuf format = arrayParser.format();

		String str = ByteBufUtils.readToString(format);
		Assert.assertEquals("*2\r\n:1\r\n+123\r\n", str);
	}



}
