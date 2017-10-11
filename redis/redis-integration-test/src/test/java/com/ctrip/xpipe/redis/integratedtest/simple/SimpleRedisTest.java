package com.ctrip.xpipe.redis.integratedtest.simple;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *
 *         Jan 20, 2017
 */
public class SimpleRedisTest extends AbstractRedisTest {

	@Test
	public void simpleTest() {

		boolean result = checkVersion(new InetSocketAddress("localhost", 2819), "2.8.19");
		logger.info("[simpleTest]{}", result);
	}

}
