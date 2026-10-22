package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.protocal.error.NoMasterlinkRedisError;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

/**
 * Phase SY (T-SY.1): PREPARE psync gate follows Manager isReadOnly (D12).
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class RedisKeeperServerStatePrepareTest extends AbstractTest {

	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Mock
	private RedisClient<?> redisClient;

	@Test
	public void testPsyncAllowedWhenReadOnly() throws Exception {
		when(redisKeeperServer.isReadOnlyStore()).thenReturn(true);
		RedisKeeperServerStatePrepare prepare = new RedisKeeperServerStatePrepare(redisKeeperServer);
		Assert.assertTrue(prepare.psync(redisClient, new String[]{"?", "-4"}));
	}

	@Test
	public void testPsyncRejectedWhenNotReadOnly() throws Exception {
		when(redisKeeperServer.isReadOnlyStore()).thenReturn(false);
		RedisKeeperServerStatePrepare prepare = new RedisKeeperServerStatePrepare(redisKeeperServer);
		try {
			prepare.psync(redisClient, new String[]{"?", "-1"});
			Assert.fail("PREPARE without read-only must reject PSYNC");
		} catch (NoMasterlinkRedisError expected) {
			Assert.assertTrue(expected.getMessage().contains("PREPARE"));
		}
	}
}
