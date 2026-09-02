package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
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
					anyBoolean(), anyBoolean(), any());
			verify(fs, never()).open(anyString(), eq(AbstractStorageFile.OpenMode.READ_WRITE),
					anyBoolean(), anyBoolean(), any());
			verify(fs, atLeastOnce()).open(contains(META_V2_FILE), eq(AbstractStorageFile.OpenMode.READ),
					eq(false), eq(true), any());
			Assert.assertEquals(runid, readOnly.dupReplicationStoreMeta().getKeeperRunid());
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
