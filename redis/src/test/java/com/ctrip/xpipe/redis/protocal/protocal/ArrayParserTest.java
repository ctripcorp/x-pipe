package com.ctrip.xpipe.redis.protocal.protocal;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.tools.ByteArrayWritableByteChannel;


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
		Integer int3 = 1024;
		String []data = new String[]{
				"*3\r\n",
				"+"+str1+"\r\n",
				"$" + str2.length() + "\r\n" + str2 + "\r\n",
				":" + int3 + "\r\n"
		};
		
		ArrayParser resultParser = (ArrayParser) parse(arrayParser, data);
		
		Object[] result = resultParser.getPayload();
		
		Assert.assertEquals(3, result.length);
		
		Assert.assertEquals(str1, result[0]);
		
		ByteArrayOutputStreamPayload bap = (ByteArrayOutputStreamPayload) result[1];
		ByteArrayWritableByteChannel channel = new ByteArrayWritableByteChannel();
		bap.out(channel);
		Assert.assertEquals(str2, new String(channel.getResult()));
		
		Assert.assertEquals(int3, (Integer)result[2]);
		
	}

}
