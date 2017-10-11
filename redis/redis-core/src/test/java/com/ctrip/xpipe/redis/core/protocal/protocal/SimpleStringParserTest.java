package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * May 12, 2016 10:03:17 AM
 */
public class SimpleStringParserTest extends AbstractRedisProtocolTest{
	
	
	private SimpleStringParser simpleStringParser = new SimpleStringParser();
	
	@Before
	public void beforeSimpleStringParser(){
		
	}

	@Test
	public void testRight(){
		
		String []contents = new String[]{
			"+",
			randomString(10),
			"\r\n",
			"nihao"
		};

		RedisClientProtocol<?> client = parse(simpleStringParser, contents);
		
		Assert.assertEquals(contents[1], client.getPayload());
	}
	
	@Test
	public void testSplit(){
		
		String[] contents = new String[]{
				"+",
				"\r",
				randomString(),
				"\n",
				"\r\n",
				randomString()
		};
		
		RedisClientProtocol<?> client = parse(simpleStringParser, contents);
		Assert.assertEquals(realContent(contents), client.getPayload());
		
		client.read(Unpooled.wrappedBuffer("123456".getBytes()));
		Assert.assertEquals(realContent(contents), client.getPayload());
		
	}

	private String realContent(String[] contents) {
		
		String result = "";
		for(int i = 1; i < contents.length ; i++){
			if(contents[i].equals("\r\n")){
				break;
			}
			result += contents[i]; 
		}
		return result;
	}
}
