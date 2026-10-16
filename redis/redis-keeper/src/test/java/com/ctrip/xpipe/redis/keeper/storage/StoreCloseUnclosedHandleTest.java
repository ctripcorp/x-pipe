package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.meta.AbstractMetaStore;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * keeper-tfs-m6 P3 (spec §3.1): {@code close(false)} returns false when a cmd / meta write handle was left
 * to the deferred closer; rdb and read handles are only deferred; {@code close()} keeps the old behaviour.
 */
public class StoreCloseUnclosedHandleTest extends AbstractRedisKeeperTest {

    private static final long WINDOW_MILLIS = 60_000L;

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private DeferredHandleCloserTest.ManualScheduler scheduler;
    private AsyncFileSystem realFs;
    private TailCacheFileSystem tcf;
    private DeferredHandleCloser closer;
    private DefaultReplicationStore store;
    private File baseDir;

    @Before
    public void setUpFs() {
        scheduler = new DeferredHandleCloserTest.ManualScheduler(clock);
        realFs = spy(createTestAsyncFileSystem());
        tcf = (TailCacheFileSystem) realFs;
        closer = new DeferredHandleCloser(realFs, () -> WINDOW_MILLIS, clock::get, scheduler.executor);
        DeferredHandleCloser.register(realFs, closer);
        baseDir = new File(getTestFileDir());
    }

    @After
    public void tearDownFs() {
        closer.shutdown(0);
        realFs.shutdown();
    }

    @Override
    protected AsyncFileSystem asyncFileSystem() {
        return realFs;
    }

    /** Store with a confirmed, still-writing rdb: cmd segment, meta and rdb write handles are open. */
    private void openStoreWithRdb() throws IOException {
        store = createDefaultReplicationStore(baseDir, new TestKeeperConfig(), randomKeeperRunid(),
                createkeeperMonitor(), Mockito.mock(SyncRateManager.class), createRedisOpParser());
        store.getMetaStore().becomeActive();
        RdbStore rdbStore = store.prepareRdb(randomKeeperRunid(), -1, new LenEofType(100));
        rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
        rdbStore.updateRdbType(RdbStore.Type.NORMAL);
        store.confirmRdb(rdbStore);
    }

    private AsyncSegmentFile cmdSegment() {
        return ((AbstractCommandStore) ReflectionTestUtils.getField(store, "cmdStore")).getWriteSegmentFile();
    }

    private static AsyncFile metaHandle(Object metaStore) {
        return (AsyncFile) ReflectionTestUtils.getField(metaStore, "metaAsyncFile");
    }

    private void failTwiceThenClose(AsyncSegmentFile segment) {
        OperationNotExecutedException one = new OperationNotExecutedException("index dirty");
        doThrow(one).doThrow(one).doCallRealMethod().when(realFs).close(same(segment), anyBoolean());
    }

    private void failTwiceThenClose(AsyncFile file) {
        OperationNotExecutedException one = new OperationNotExecutedException("in-flight");
        doThrow(one).doThrow(one).doCallRealMethod().when(realFs).close(same(file), anyBoolean());
    }

    private void drainCloser() {
        while (closer.count() > 0) {
            scheduler.runNext();
        }
    }

    @Test
    public void testDeferredCmdWriteHandleIsReportedAfterFullCleanup() throws Exception {
        openStoreWithRdb();
        AsyncSegmentFile segment = cmdSegment();
        AbstractMetaStore metaStore = (AbstractMetaStore) store.getMetaStore();
        failTwiceThenClose(segment);

        assertFalse(store.close(false));
        // cmd not closed did not skip meta: meta closed normally, only the segment is with the closer.
        assertFalse(metaStore.isMetaHandleOpen());
        assertEquals(1, closer.count());
        assertTrue(closer.isPending(segment));

        drainCloser();
        assertEquals("tracker committed returned", 0, tcf.getGlobalCommittedBytes());
    }

    @Test
    public void testDeferredMetaWriteHandleIsReported() throws Exception {
        openStoreWithRdb();
        AsyncFile meta = metaHandle(store.getMetaStore());
        failTwiceThenClose(meta);

        assertFalse(store.close(false));
        assertTrue(closer.isPending(meta));
        assertEquals(1, closer.writeCount());
        drainCloser();
        assertEquals(0, tcf.getGlobalCommittedBytes());
    }

    @Test
    public void testIgnoreCloseFailReturnsTrueAndStillDefers() throws Exception {
        openStoreWithRdb();
        AsyncSegmentFile segment = cmdSegment();
        failTwiceThenClose(segment);

        assertTrue(store.close(true));
        assertTrue(closer.isPending(segment));
        drainCloser();
    }

    @Test
    public void testPlainCloseNeitherThrowsNorReports() throws Exception {
        openStoreWithRdb();
        AsyncFile meta = metaHandle(store.getMetaStore());
        failTwiceThenClose(meta);

        store.close();
        assertTrue(closer.isPending(meta));
        drainCloser();
    }

    @Test
    public void testDeferredRdbWriteHandleIsNotReported() throws Exception {
        openStoreWithRdb();
        RdbStore rdbStore = store.getRdbStore();
        AsyncFile rdbWrite = (AsyncFile) ReflectionTestUtils.getField(rdbStore, "writeAsyncFile");
        assertNotNull(rdbWrite);
        failTwiceThenClose(rdbWrite);

        assertTrue(store.close(false));
        assertTrue(closer.isPending(rdbWrite));
        assertEquals(1, closer.writeCount());

        drainCloser();
        assertEquals(0, tcf.getGlobalCommittedBytes());
    }

    @Test
    public void testDeferredReadOnlyMetaHandleIsNotReported() throws Exception {
        DefaultMetaStore writer = new DefaultMetaStore(baseDir, randomKeeperRunid(), realFs, getReplId());
        writer.initialize();
        writer.close();

        DefaultMetaStore reader = new DefaultMetaStore(baseDir, randomKeeperRunid(), realFs, getReplId(), true);
        reader.initialize();
        AsyncFile meta = metaHandle(reader);
        assertFalse(meta.canWrite());
        failTwiceThenClose(meta);

        assertTrue(reader.close(false));
        assertTrue(closer.isPending(meta));
        assertEquals(0, closer.writeCount());
        drainCloser();
    }

    @Test
    public void testRuntimeFailureInCmdStoreDoesNotSkipMeta() throws Exception {
        openStoreWithRdb();
        ((CommandStore) ReflectionTestUtils.getField(store, "cmdStore")).close();
        CommandStore broken = mock(CommandStore.class);
        doThrow(new IllegalStateException("broken cmd close")).when(broken).close(false);
        ReflectionTestUtils.setField(store, "cmdStore", broken);
        AbstractMetaStore metaStore = (AbstractMetaStore) store.getMetaStore();

        try {
            store.close(false);
            fail("expected IOException");
        } catch (IOException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
        assertFalse("meta closed despite the cmd failure", metaStore.isMetaHandleOpen());
        assertEquals(0, closer.count());
    }

    @Test
    public void testRepeatedCloseAfterReportReturnsTrueWithoutFsCall() throws Exception {
        openStoreWithRdb();
        AsyncSegmentFile segment = cmdSegment();
        failTwiceThenClose(segment);
        assertFalse(store.close(false));
        CommandStore cmdStore = (CommandStore) ReflectionTestUtils.getField(store, "cmdStore");
        clearInvocations(realFs);

        assertTrue(store.close(false));
        assertTrue(cmdStore.close(false));
        assertTrue(store.getMetaStore().close(false));
        verify(realFs, never()).close(same(segment), anyBoolean());
        assertEquals(1, closer.count());
        drainCloser();
    }

    @Test
    public void testMetaDestroyDeletesWhenCloseIsDeferred() throws Exception {
        DefaultMetaStore metaStore = new DefaultMetaStore(baseDir, randomKeeperRunid(), realFs, getReplId());
        metaStore.initialize();
        AsyncFile meta = metaHandle(metaStore);
        failTwiceThenClose(meta);

        metaStore.destroy();
        assertTrue(closer.isPending(meta));
        assertFalse(new File(baseDir, "meta.v2.json").exists());
        drainCloser();
        assertEquals(0, tcf.getGlobalCommittedBytes());
    }
}
