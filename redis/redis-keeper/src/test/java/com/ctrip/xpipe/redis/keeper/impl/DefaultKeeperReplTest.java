package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import org.junit.Assert;
import org.junit.Test;

/**
 * A null store (read-only open without a cmd chain, or released before the sync executor runs) must read as
 * "fresh" so GapAllowSyncHandler.anaRequest replies "replicationstore fresh" instead of NPE.
 */
public class DefaultKeeperReplTest {

	@Test
	public void testNullStoreReadsAsFresh() {
		DefaultKeeperRepl repl = new DefaultKeeperRepl(null);
		Assert.assertNull(repl.preStage());
		Assert.assertNull(repl.currentStage());
		Assert.assertEquals(ReplicationStoreMeta.DEFAULT_END_OFFSET, repl.getEndOffset());
	}
}
