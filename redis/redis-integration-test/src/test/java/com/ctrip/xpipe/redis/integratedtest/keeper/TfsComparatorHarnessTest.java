package com.ctrip.xpipe.redis.integratedtest.keeper;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Phase CH 冒烟：进程内两路 {@code ? -4} 对齐后写 Redis 增量，{@code comparedBytes} 增长且无失配（AC-23 最小闭环）。
 */
public class TfsComparatorHarnessTest extends AbstractTfsKeeperIntegrated {

	private static final int INCREMENT_KEYS = 20;

	@Test
	public void testComparedBytesGrowWithoutMismatch() throws Exception {
		startComparator();
		waitAligned();
		long from = comparatorHarness.comparedBytes();

		try (Jedis jedis = createJedis(getRedisMaster())) {
			for (int i = 0; i < INCREMENT_KEYS; i++) {
				jedis.set("tfs-ch-" + i, randomString(16));
			}
		}

		waitComparedBytesGrow(from);
		Assert.assertEquals(0L, comparatorHarness.mismatchCount());
		Assert.assertTrue(comparatorHarness.comparedBytes() > from);
	}

}
