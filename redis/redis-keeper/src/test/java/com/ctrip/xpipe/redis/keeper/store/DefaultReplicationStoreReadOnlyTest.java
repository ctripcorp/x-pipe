package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.CommandReaderWriterFactory;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.GtidCmdFilter;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.readonly.ReadOnlyCommandStore;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

/**
 * Phase RS (T-RS.4): read-only ReplicationStore assembly. AC-2 / AC-2d ⑤ / AC-6b.
 */
public class DefaultReplicationStoreReadOnlyTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private TestKeeperConfig keeperConfig;

	@Before
	public void beforeDefaultReplicationStoreReadOnlyTest() {
		keeperConfig = new TestKeeperConfig();
		keeperConfig.setReplicationStoreGcIntervalSeconds(1);
		keeperConfig.setMinTimeMilliToGcAfterCreate(3000);
	}

	@Test
	public void testGtidReadOnlyConstructsReadOnlyCommandStoreAndSkipsOverride() throws Exception {
		File baseDir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		seedWritableStore(baseDir, runid);

		AtomicBoolean gtidCreateCalled = new AtomicBoolean(false);
		GtidReplicationStore readOnly = new GtidReplicationStore(baseDir, new DefaultKeeperConfig(), runid,
				createkeeperMonitor(), createRedisOpParser(), mock(SyncRateManager.class), null,
				asyncFileSystem(), getReplId(), true) {
			@Override
			protected CommandStore createCommandStore(File baseDir, ReplicationStoreMeta replMeta, int cmdFileSize,
													  KeeperConfig config, CommandReaderWriterFactory factory,
													  KeeperMonitor keeperMonitor, GtidCmdFilter filter) throws java.io.IOException {
				gtidCreateCalled.set(true);
				return super.createCommandStore(baseDir, replMeta, cmdFileSize, config, factory, keeperMonitor, filter);
			}
		};
		try {
			Assert.assertFalse("GtidReplicationStore.createCommandStore must be unreachable in read-only",
					gtidCreateCalled.get());
			Assert.assertTrue(readOnly.cmdStore instanceof ReadOnlyCommandStore);
			Assert.assertNull(readOnly.getRdbStore());
			Assert.assertNull(readOnly.rordbStoreRef.get());
			readOnly.backlogEndOffset();
		} finally {
			readOnly.close();
		}
	}

	@Test
	public void testManagerGetCurrentOpensReadOnlyCommandStore() throws Exception {
		DefaultReplicationStoreManager manager = new DefaultReplicationStoreManager(
				keeperConfig, getReplId(), randomKeeperRunid(), new File(getTestFileDir()),
				createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), null, asyncFileSystem());
		LifecycleHelper.initializeIfPossible(manager);
		LifecycleHelper.startIfPossible(manager);
		try {
			GtidReplicationStore writable = (GtidReplicationStore) manager.create();
			seedCommands(writable);
			File storeDir = writable.getBaseDir();

			LifecycleHelper.stopIfPossible(manager);
			manager.setReadOnly(true);
			LifecycleHelper.startIfPossible(manager);

			ReplicationStore opened = manager.getCurrent();
			Assert.assertTrue(opened instanceof GtidReplicationStore);
			GtidReplicationStore readOnly = (GtidReplicationStore) opened;
			Assert.assertEquals(storeDir, readOnly.getBaseDir());
			Assert.assertTrue(readOnly.cmdStore instanceof ReadOnlyCommandStore);
			Assert.assertNull(readOnly.getRdbStore());
		} finally {
			LifecycleHelper.stopIfPossible(manager);
			LifecycleHelper.disposeIfPossible(manager);
		}
	}

	@Test
	public void testReadOnlyWriteEntriesThrow() throws Exception {
		GtidReplicationStore store = new GtidReplicationStore(new File(getTestFileDir()), new DefaultKeeperConfig(),
				randomKeeperRunid(), createkeeperMonitor(), createRedisOpParser(), mock(SyncRateManager.class),
				null, asyncFileSystem(), getReplId(), true);
		try {
			assertReadOnlyStore(() -> store.appendCommands(Unpooled.wrappedBuffer(new byte[]{1})));
			assertReadOnlyStore(() -> store.prepareRdb(REPL_ID, 0, new LenEofType(1)));
			assertReadOnlyStore(() -> store.prepareRdb(REPL_ID, 0, new LenEofType(1), ReplStage.ReplProto.PSYNC,
					new GtidSet(GtidSet.EMPTY_GTIDSET), "uuid"));
			assertReadOnlyStore(() -> store.confirmRdb(mock(RdbStore.class)));
			assertReadOnlyStore(() -> store.confirmRdbGapAllowed(mock(RdbStore.class)));
			assertReadOnlyStore(() -> store.switchToPSync(REPL_ID, 0));
			assertReadOnlyStore(() -> store.switchToXSync(REPL_ID, 0, "uuid",
					new GtidSet(GtidSet.EMPTY_GTIDSET), new GtidSet(GtidSet.EMPTY_GTIDSET)));
			assertReadOnlyStore(store::gc);
			assertReadOnlyStore(store::destroy);
			Assert.assertTrue(store.isFresh());
		} finally {
			store.close();
		}
	}

	private void seedWritableStore(File baseDir, String runid) throws Exception {
		GtidReplicationStore writable = new GtidReplicationStore(baseDir, new DefaultKeeperConfig(), runid,
				createkeeperMonitor(), createRedisOpParser(), mock(SyncRateManager.class), null,
				asyncFileSystem(), getReplId());
		try {
			RdbStore rdbStore = writable.prepareRdb(REPL_ID, 0, new LenEofType(8), ReplStage.ReplProto.PSYNC,
					new GtidSet(GtidSet.EMPTY_GTIDSET), null);
			rdbStore.updateRdbType(RdbStore.Type.NORMAL);
			rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
			writable.confirmRdbGapAllowed(rdbStore);
			writable.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes()));
		} finally {
			writable.close();
		}
	}

	private void seedCommands(GtidReplicationStore store) throws Exception {
		store.psyncContinueFrom(REPL_ID, 1);
		store.appendCommands(Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes()));
	}

	private static void assertReadOnlyStore(ThrowingRunnable action) throws Exception {
		try {
			action.run();
			Assert.fail("expected IllegalStateException(read only store)");
		} catch (IllegalStateException e) {
			Assert.assertEquals(DefaultReplicationStore.READ_ONLY_STORE_MSG, e.getMessage());
		}
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}
}
