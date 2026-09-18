package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Phase TB 冒烟：TFS 共用目录 + Active/Prepare 拓扑（T-TB.3）。
 */
public class TfsKeeperTopologyTest extends AbstractTfsKeeperIntegrated {

	@Test
	public void testTfsTopologySharesStoreDirAndPrepareWatch() throws Exception {
		DefaultRedisKeeperServer active = tfsKeeperServer(activeKeeper);
		DefaultRedisKeeperServer prepare = tfsKeeperServer(backupKeeper);
		Assert.assertNotNull(active);
		Assert.assertNotNull(prepare);

		Assert.assertTrue(active.isTfsMode());
		Assert.assertTrue(prepare.isTfsMode());
		Assert.assertTrue(prepare.isReadOnlyStore());
		Assert.assertFalse(active.isReadOnlyStore());

		ReplicationStoreManager activeManager = tfsStoreManager(activeKeeper);
		ReplicationStoreManager prepareManager = tfsStoreManager(backupKeeper);
		File expected = expectedManagerBaseDir().getCanonicalFile();
		Assert.assertEquals(expected, activeManager.getBaseDir().getCanonicalFile());
		Assert.assertEquals(expected, prepareManager.getBaseDir().getCanonicalFile());

		Assert.assertTrue(configGetPrepareWatch(activeKeeper));
		Assert.assertTrue(configGetPrepareWatch(backupKeeper));

		String activeDump = describeTfsActiveWait(activeKeeper);
		Assert.assertTrue(activeDump, activeDump.contains(getClusterId()));
		Assert.assertTrue(activeDump, activeDump.contains("masterState="));
		Assert.assertTrue(activeDump, activeDump.contains("checkOk="));
		String prepareDump = describeTfsPrepareWait(backupKeeper);
		Assert.assertTrue(prepareDump, prepareDump.contains(getShardId()));
		Assert.assertTrue(prepareDump, prepareDump.contains("keeperState="));
		Assert.assertTrue(prepareDump, prepareDump.contains("readOnlyStore="));
	}

}
