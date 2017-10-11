package com.ctrip.xpipe.redis.core.protocal.protocal;


import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import org.junit.Assert;
import org.junit.Test;



/**
 * @author wenchao.meng
 *
 * Sep 14, 2016
 */
public class ParserManagerTest extends AbstractRedisTest{
	
	@Test
	public void test(){
		
		Assert.assertEquals(":1\r\n", ByteBufUtils.readToString(ParserManager.parse(1L)));
		Assert.assertEquals("+nihao\r\n", ByteBufUtils.readToString(ParserManager.parse("nihao")));
		Assert.assertEquals("-error\r\n", ByteBufUtils.readToString(ParserManager.parse(new RedisError("error"))));
		Assert.assertEquals("*2\r\n+str\r\n:1\r\n", ByteBufUtils.readToString(ParserManager.parse(new Object[]{"str", 1L})));
		Assert.assertEquals("$5\r\nnihao\r\n", ByteBufUtils.readToString(ParserManager.parse(new ByteArrayOutputStreamPayload("nihao"))));


		int a = 1;
		Assert.assertEquals("*2\r\n+str\r\n:1\r\n", ByteBufUtils.readToString(ParserManager.parse(new Object[]{"str", a})));

	}
	

}
