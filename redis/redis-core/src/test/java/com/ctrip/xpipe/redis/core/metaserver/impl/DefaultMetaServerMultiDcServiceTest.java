package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Nov 3, 2016
 */
public class DefaultMetaServerMultiDcServiceTest extends AbstractRedisTest {

	private int retryTimes = 1;
	private int retryIntervalMilli = 10;

	@Test
	public void testRetry() {

		String metaServerAddress = String.format("http://localhost:%d", randomPort());

		DefaultMetaServerMultiDcService metaServerMultiDcService = new DefaultMetaServerMultiDcService(
				metaServerAddress, retryTimes, retryIntervalMilli);
		Assert.assertNull(metaServerMultiDcService.getActiveKeeper("clusterId", "shardId"));

	}

	@Test
	public void testRetry502() {

		String metaServerAddress = String.format("http://localhost");

		DefaultMetaServerMultiDcService metaServerMultiDcService = new DefaultMetaServerMultiDcService(
				metaServerAddress, retryTimes, retryIntervalMilli);
		Assert.assertNull(metaServerMultiDcService.getActiveKeeper("clusterId", "shardId"));

	}

	@Test
	public void testOtherNoRetry() {

		String metaServerAddress = String.format("http://localhost/404");

		DefaultMetaServerMultiDcService metaServerMultiDcService = new DefaultMetaServerMultiDcService(
				metaServerAddress, retryTimes, retryIntervalMilli);
		Assert.assertNull(metaServerMultiDcService.getActiveKeeper("clusterId", "shardId"));
	}

}
