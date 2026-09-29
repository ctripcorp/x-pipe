package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystemConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * Phase CH review：TFS IT 共用一份生产 TailCache FS，不每路 {@code new}。
 */
public class AbstractTfsKeeperIntegratedTest {

	@Test
	public void testTfsIntegratedFileSystemSharedProductionChunk() {
		TestKeeperConfig config = new TestKeeperConfig();
		AsyncFileSystem fs1 = AbstractTfsKeeperIntegrated.sharedTfsIntegratedFileSystem(config);
		AsyncFileSystem fs2 = AbstractTfsKeeperIntegrated.sharedTfsIntegratedFileSystem(config);
		Assert.assertSame(fs1, fs2);
		Assert.assertTrue(fs1 instanceof TailCacheFileSystem);
		Assert.assertEquals(new TailCacheFileSystemConfig().getChunkSize(),
				((TailCacheFileSystem) fs1).getChunkSize());
	}
}
