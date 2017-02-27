package com.ctrip.xpipe.redis.integratedtest.simple;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;

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
