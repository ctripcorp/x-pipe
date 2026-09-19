package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;

import static com.ctrip.xpipe.redis.core.store.MetaStore.META_V2_FILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Phase RS (T-RS.3 / T-RS.4): AbstractMetaStore read-only. AC-2.
 */
public class DefaultMetaStoreReadOnlyTest extends AbstractRedisKeeperTest {

	private static final String REPL_ID = "000000000000000000000000000000000000000A";

	private static final String CMD_PREFIX = "cmd_rs_readonly_";

	@Test
	public void testReloadReadOnlyMetaDoesNotCloseHandleItself() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		File dir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultMetaStore writable = new DefaultMetaStore(dir, runid, fs, getReplId());
		writable.initialize();
		writable.rdbConfirmPsync(REPL_ID, 100, 0, "rdb_x", RdbStore.Type.NORMAL, new LenEofType(10), CMD_PREFIX);
		writable.close();

		DefaultMetaStore readOnly = new DefaultMetaStore(dir, runid, fs, getReplId(), true);
		readOnly.initialize();
		try {
			clearInvocations(fs);
			readOnly.reloadReadOnlyMeta();
			verify(fs, never()).close(any(AsyncFile.class));

			readOnly.closeReadOnlyMetaHandle();
			verify(fs, atLeastOnce()).close(any(AsyncFile.class));
			// 幂等：句柄已关，再关不会重复 close
			clearInvocations(fs);
			readOnly.closeReadOnlyMetaHandle();
			verify(fs, never()).close(any(AsyncFile.class));
		} finally {
			readOnly.close();
			fs.shutdown();
		}
	}

	@Test
	public void testReloadEntriesDoNotCloseHandleInSource() throws Exception {
		assertMethodBodyHasNoClose(
				new File("src/main/java/com/ctrip/xpipe/redis/keeper/store/meta/AbstractMetaStore.java"),
				"public void reloadReadOnlyMeta()", "closeHandle");
		assertMethodBodyHasNoClose(
				new File("src/main/java/com/ctrip/xpipe/redis/keeper/store/DefaultReplicationStoreManager.java"),
				"public synchronized String reloadLatestStoreDir()", "closeManagerMetaFile");
	}

	private static void assertMethodBodyHasNoClose(File source, String signature, String forbidden) throws Exception {
		Assert.assertTrue("missing " + source.getPath(), source.isFile());
		String text = new String(java.nio.file.Files.readAllBytes(source.toPath()),
				java.nio.charset.StandardCharsets.UTF_8);
		int start = text.indexOf(signature);
		Assert.assertTrue("signature not found: " + signature, start >= 0);
		int open = text.indexOf('{', start);
		Assert.assertTrue(open > start);
		int depth = 0;
		int end = -1;
		for (int i = open; i < text.length(); i++) {
			char c = text.charAt(i);
			if (c == '{') {
				depth++;
			} else if (c == '}') {
				depth--;
				if (depth == 0) {
					end = i;
					break;
				}
			}
		}
		Assert.assertTrue("unbalanced braces for " + signature, end > open);
		String body = text.substring(open, end);
		Assert.assertFalse(signature + " must not call " + forbidden, body.contains(forbidden));
	}

	@Test
	public void testReadOnlyParseMatchesProduction() throws Exception {
		File dir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultMetaStore writable = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId());
		writable.initialize();
		writable.rdbConfirmPsync(REPL_ID, 100, 0, "rdb_x", RdbStore.Type.NORMAL, new LenEofType(10), CMD_PREFIX);
		writable.close();

		DefaultMetaStore readOnly = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId(), true);
		readOnly.initialize();
		DefaultMetaStore production = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId());
		production.initialize();
		try {
			ReplicationStoreMeta ro = readOnly.dupReplicationStoreMeta();
			ReplicationStoreMeta rw = production.dupReplicationStoreMeta();
			Assert.assertEquals(rw.getBeginOffset(), ro.getBeginOffset());
			Assert.assertEquals(rw.getCurReplStage().getReplId(), ro.getCurReplStage().getReplId());
			Assert.assertEquals(rw.getCmdFilePrefix(), ro.getCmdFilePrefix());
			Assert.assertEquals(CMD_PREFIX, ro.getCmdFilePrefix());
			Assert.assertEquals(REPL_ID, ro.getCurReplStage().getReplId());
		} finally {
			readOnly.close();
			production.close();
		}
	}

	@Test
	public void testReadOnlyOpensMetaWithReadModeAndSkipsRunidWrite() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		File dir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultMetaStore writable = new DefaultMetaStore(dir, runid, fs, getReplId());
		writable.initialize();
		writable.rdbConfirmPsync(REPL_ID, 100, 0, "rdb_x", RdbStore.Type.NORMAL, new LenEofType(10), CMD_PREFIX);
		writable.close();
		clearInvocations(fs);

		DefaultMetaStore readOnly = new DefaultMetaStore(dir, "different-runid-should-not-write", fs, getReplId(), true);
		readOnly.initialize();
		try {
			verify(fs, never()).mkdir(anyString(), anyBoolean());
			verify(fs, never()).open(anyString(), eq(AbstractStorageFile.OpenMode.WRITE),
					any(AbstractStorageFile.ReplaceMode.class), anyBoolean(), any());
			verify(fs, never()).open(anyString(), eq(AbstractStorageFile.OpenMode.READ_WRITE),
					any(AbstractStorageFile.ReplaceMode.class), anyBoolean(), any());
			verify(fs, atLeastOnce()).open(contains(META_V2_FILE), eq(AbstractStorageFile.OpenMode.READ),
					eq(AbstractStorageFile.ReplaceMode.ATOMIC_PREFER_TMP), eq(true), any());
			Assert.assertEquals(runid, readOnly.dupReplicationStoreMeta().getKeeperRunid());
		} finally {
			readOnly.close();
			fs.shutdown();
		}
	}

	@Test
	public void testGetterDoesNotReloadMeta() throws Exception {
		File dir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultMetaStore readOnly = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId(), true);
		readOnly.initialize();
		try {
			Assert.assertNull(readOnly.getCurrentReplStage());

			DefaultMetaStore writable = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId());
			writable.initialize();
			writable.rdbConfirmPsync(REPL_ID, 100, 0, "rdb_x", RdbStore.Type.NORMAL, new LenEofType(10), CMD_PREFIX);
			writable.close();

			Assert.assertNull(readOnly.getCurrentReplStage());
			Assert.assertNull(readOnly.getPreReplStage());
			Assert.assertNull(readOnly.getReplId());
		} finally {
			readOnly.close();
		}
	}

	@Test
	public void testReloadReadOnlyMetaRefreshesWholeMetaRef() throws Exception {
		File dir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultMetaStore readOnly = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId(), true);
		readOnly.initialize();
		try {
			Assert.assertNull(readOnly.dupReplicationStoreMeta().getCurReplStage());

			DefaultMetaStore writable = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId());
			writable.initialize();
			writable.rdbConfirmPsync(REPL_ID, 100, 0, "rdb_x", RdbStore.Type.NORMAL, new LenEofType(10), CMD_PREFIX);
			writable.close();

			// D48：reload 不再自关句柄，调用方（Watcher 关相）必须先关
			readOnly.closeReadOnlyMetaHandle();
			readOnly.reloadReadOnlyMeta();
			Assert.assertEquals(REPL_ID, readOnly.getCurrentReplStage().getReplId());
			Assert.assertEquals(REPL_ID, readOnly.getCurReplStageReplId());
			Assert.assertEquals(CMD_PREFIX, readOnly.dupReplicationStoreMeta().getCmdFilePrefix());
		} finally {
			readOnly.close();
		}
	}

	@Test
	public void testReloadReadOnlyMetaUpdatesAfterCurrentStagePresent() throws Exception {
		File dir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultMetaStore writable = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId());
		writable.initialize();
		writable.rdbConfirmPsync(REPL_ID, 100, 0, "rdb_x", RdbStore.Type.NORMAL, new LenEofType(10), CMD_PREFIX);
		writable.close();

		DefaultMetaStore readOnly = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId(), true);
		readOnly.initialize();
		try {
			Assert.assertEquals(REPL_ID, readOnly.getCurrentReplStage().getReplId());

			writable = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId());
			writable.initialize();
			writable.shiftReplicationId("000000000000000000000000000000000000000B", 100L);
			writable.close();

			Assert.assertEquals(REPL_ID, readOnly.getCurrentReplStage().getReplId());
			readOnly.closeReadOnlyMetaHandle();
			readOnly.reloadReadOnlyMeta();
			Assert.assertEquals("000000000000000000000000000000000000000B", readOnly.getReplId());
		} finally {
			readOnly.close();
		}
	}

	@Test
	public void testReadOnlyGettersDoNotReopenWhenCurrentStagePresent() throws Exception {
		AsyncFileSystem fs = spy(createTestAsyncFileSystem());
		File dir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultMetaStore writable = new DefaultMetaStore(dir, runid, fs, getReplId());
		writable.initialize();
		writable.rdbConfirmPsync(REPL_ID, 100, 0, "rdb_x", RdbStore.Type.NORMAL, new LenEofType(10), CMD_PREFIX);
		writable.close();
		clearInvocations(fs);

		DefaultMetaStore readOnly = new DefaultMetaStore(dir, runid, fs, getReplId(), true);
		readOnly.initialize();
		try {
			clearInvocations(fs);
			Assert.assertEquals(REPL_ID, readOnly.getCurrentReplStage().getReplId());
			Assert.assertEquals(REPL_ID, readOnly.getCurrentReplStage().getReplId());
			Assert.assertEquals(REPL_ID, readOnly.getCurReplStageReplId());
			verify(fs, never()).open(contains(META_V2_FILE), eq(AbstractStorageFile.OpenMode.READ),
					eq(AbstractStorageFile.ReplaceMode.NORMAL), eq(true), any());
		} finally {
			readOnly.close();
			fs.shutdown();
		}
	}

	@Test
	public void testReadOnlyWriteEntriesThrow() throws Exception {
		File dir = new File(getTestFileDir());
		String runid = randomKeeperRunid();
		DefaultMetaStore writable = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId());
		writable.initialize();
		writable.rdbConfirmPsync(REPL_ID, 100, 0, "rdb_x", RdbStore.Type.NORMAL, new LenEofType(10), CMD_PREFIX);
		writable.close();

		DefaultMetaStore readOnly = new DefaultMetaStore(dir, runid, asyncFileSystem(), getReplId(), true);
		readOnly.initialize();
		try {
			ReplicationStoreMeta meta = readOnly.dupReplicationStoreMeta();
			assertReadOnlyStore(() -> readOnly.saveMetaToFileV2(new File(dir, META_V2_FILE), meta));
			assertReadOnlyStore(() -> readOnly.saveMeta(meta));
			assertReadOnlyStore(() -> readOnly.saveMeta(meta, meta));
			assertReadOnlyStore(() -> readOnly.setMasterAddress(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 6379))));
			assertReadOnlyStore(readOnly::becomeActive);
			assertReadOnlyStore(readOnly::becomeBackup);
		} finally {
			readOnly.close();
		}
	}

	private static void assertReadOnlyStore(ThrowingRunnable action) throws Exception {
		try {
			action.run();
			Assert.fail("expected IllegalStateException(read only store)");
		} catch (IllegalStateException e) {
			Assert.assertEquals(AbstractMetaStore.READ_ONLY_STORE_MSG, e.getMessage());
		}
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}
}
