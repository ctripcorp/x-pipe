package com.ctrip.xpipe.redis.core.metaserver.impl;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;

/**
 * @author wenchao.meng
 *
 *         Nov 3, 2016
 */
public class DefaultMetaServerMultiDcServiceTest extends AbstractRedisTest {

	private int retryTimes = 5;
	private int retryIntervalMilli = 10;

	@Test
	public void testRetry() {

		String metaServerAddress = String.format("http://localhost:%d", randomPort());

		DefaultMetaServerMultiDcService metaServerMultiDcService = new DefaultMetaServerMultiDcService(
				metaServerAddress, retryTimes, retryIntervalMilli);
		Assert.assertNull(metaServerMultiDcService.getActiveKeeper("clusterId", "shardId"));
		;

	}

}
