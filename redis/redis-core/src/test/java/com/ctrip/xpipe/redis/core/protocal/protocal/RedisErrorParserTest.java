package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.error.NoMasterlinkRedisError;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Mar 6, 2017
 */
public class RedisErrorParserTest extends AbstractRedisTest {

	@Test
	public void testRedisError() {

		String message = "Can't SYNC while replicationstore fresh";
		RedisError redisError = new NoMasterlinkRedisError(message);
		ByteBuf byteBuf = new RedisErrorParser(redisError).format();
		String result = ByteBufUtils.readToString(byteBuf);
		Assert.assertEquals("-" + redisError.getMessage() + "\r\n", result);

	}

}
