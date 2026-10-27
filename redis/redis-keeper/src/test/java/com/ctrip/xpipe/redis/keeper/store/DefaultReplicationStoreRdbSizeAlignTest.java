package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofMarkType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.LogCapture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.core.store.MetaStore.META_V2_FILE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DefaultReplicationStoreRdbSizeAlignTest extends AbstractRedisKeeperTest {

    private static final int DATA_LEN = 1500;

    private AsyncFileSystem fs;
    private File baseDir;
    private final String runid = randomKeeperRunid();
    private DefaultReplicationStore store;

    private final AtomicBoolean failMetaWrites = new AtomicBoolean(false);
    private final AtomicInteger metaWrites = new AtomicInteger();

    @Before
    public void setUpStore() throws IOException {
        fs = spy(createTestAsyncFileSystem());
        doAnswer(inv -> {
            AsyncFile file = inv.getArgument(0);
            String path = (String) ReflectionTestUtils.getField(file, "path");
            if (path != null && path.endsWith(META_V2_FILE)) {
                metaWrites.incrementAndGet();
                if (failMetaWrites.get()) {
                    ByteBuf buf = inv.getArgument(1);
                    if (buf != null && buf.refCnt() > 0) {
                        buf.release();
                    }
                    return CompletableFuture.failedFuture(new IOException("injected meta write fail"));
                }
            }
            return inv.callRealMethod();
        }).when(fs).write(any(AsyncFile.class), any(ByteBuf.class));

        baseDir = new File(getTestFileDir());
        store = createDefaultReplicationStore(baseDir, new TestKeeperConfig(), runid, createkeeperMonitor(),
                mock(SyncRateManager.class), createRedisOpParser());
        store.getMetaStore().becomeActive();
    }

    @After
    public void tearDownStore() throws Exception {
        failMetaWrites.set(false);
        if (store != null) {
            try {
                store.close();
            } catch (Exception e) {
                logger.warn("[tearDownStore] close store", e);
            }
        }
        fs.shutdown();
    }

    @Override
    protected AsyncFileSystem asyncFileSystem() {
        return fs;
    }

    @Test
    public void testRdbSizeAlignedAfterOnEndRdbFailureAndReopenSucceeds() throws Exception {
        alignedAfterOnEndRdbFailureAndReopenSucceeds(RdbStore.Type.NORMAL);
    }

    @Test
    public void testRordbSizeAlignedAfterOnEndRdbFailureAndReopenSucceeds() throws Exception {
        alignedAfterOnEndRdbFailureAndReopenSucceeds(RdbStore.Type.RORDB);
    }

    private void alignedAfterOnEndRdbFailureAndReopenSucceeds(RdbStore.Type type) throws Exception {
        RdbStore rdb = endedEofMarkRdbWithSizeMissing(type);

        store.flushMeta();
        assertEquals("aligned to the in-memory rdb length", DATA_LEN, metaSize(type));
        assertEquals(rdb.getRdbFile().getName(), metaFile(type));

        store.close();
        store = null;
        AsyncFileSystem fresh = createTestAsyncFileSystem();
        DefaultReplicationStore reopened = new DefaultReplicationStore(baseDir, new TestKeeperConfig(), runid,
                createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser(), fresh, getReplId());
        try {
            assertEquals(DATA_LEN, metaSize(reopened, type));
            RdbStore recovered = storeRef(reopened, type).get();
            assertNotNull("rdb recovered on reopen", recovered);
            assertTrue("recovered rdb is Success", recovered.checkOk());
        } finally {
            reopened.close();
            fresh.shutdown();
        }
    }

    @Test
    public void testWritingRdbNotAligned() throws Exception {
        for (RdbStore.Type type : RdbStore.Type.values()) {
            if (type == RdbStore.Type.UNKNOWN) {
                continue;
            }
            RdbStore rdb = confirmRdb(type, new EofMarkType(randomKeeperRunid()));
            rdb.writeRdb(Unpooled.wrappedBuffer(randomString(DATA_LEN).getBytes()));
            assertTrue(rdb.isWriting());

            assertOnlyPeriodicRewrite(type);
            assertEquals(type + ": writing rdb keeps -1", -1, metaSize(type));
        }
    }

    @Test
    public void testFailedRdbNotAligned() throws Exception {
        RdbStore rdb = confirmRdb(RdbStore.Type.NORMAL, new EofMarkType(randomKeeperRunid()));
        rdb.writeRdb(Unpooled.wrappedBuffer(randomString(DATA_LEN).getBytes()));
        rdb.failRdb(new IOException("injected rdb fail"));
        assertEquals(-1, metaSize(RdbStore.Type.NORMAL));

        assertOnlyPeriodicRewrite(RdbStore.Type.NORMAL);
        assertEquals(-1, metaSize(RdbStore.Type.NORMAL));
    }

    @Test
    public void testReleasedRdbNotAligned() throws Exception {
        endedEofMarkRdbWithSizeMissing(RdbStore.Type.NORMAL);
        store.releaseRdb();
        assertNull(store.getRdbStore());
        assertNull(metaFile(RdbStore.Type.NORMAL));

        assertOnlyPeriodicRewrite(RdbStore.Type.NORMAL);
        assertNull(metaFile(RdbStore.Type.NORMAL));
        assertNotEquals(DATA_LEN, metaSize(RdbStore.Type.NORMAL));
    }

    @Test
    public void testMissingRdbFileNotAligned() throws Exception {
        RdbStore rdb = endedEofMarkRdbWithSizeMissing(RdbStore.Type.NORMAL);
        String path = rdb.getRdbFile().getAbsolutePath();
        AsyncFileSystemHelper.await(() -> fs.delete(path), "delete rdb " + path);

        assertOnlyPeriodicRewrite(RdbStore.Type.NORMAL);
        assertEquals(-1, metaSize(RdbStore.Type.NORMAL));
    }

    @Test
    public void testLenRdbNotRewritten() throws Exception {
        RdbStore rdb = confirmRdb(RdbStore.Type.NORMAL, new LenEofType(DATA_LEN));
        rdb.writeRdb(Unpooled.wrappedBuffer(randomString(DATA_LEN).getBytes()));
        rdb.endRdb();
        assertTrue(rdb.checkOk());
        assertEquals(DATA_LEN, metaSize(RdbStore.Type.NORMAL));
        RdbStore spyRdb = spy(rdb);
        assertTrue(storeRef(store, RdbStore.Type.NORMAL).compareAndSet(rdb, spyRdb));

        assertOnlyPeriodicRewrite(RdbStore.Type.NORMAL);
        verify(spyRdb, never()).checkOk();
        verify(spyRdb, never()).rdbFileLength();
        assertEquals(DATA_LEN, metaSize(RdbStore.Type.NORMAL));
    }

    @Test
    public void testRdbSwitchDuringCheckKeepsNewRdbSize() throws Exception {
        switchDuringCheckKeepsNewRdbSize(RdbStore.Type.NORMAL);
    }

    @Test
    public void testRordbSwitchDuringCheckKeepsNewRordbSize() throws Exception {
        switchDuringCheckKeepsNewRdbSize(RdbStore.Type.RORDB);
    }

    private void switchDuringCheckKeepsNewRdbSize(RdbStore.Type type) throws Exception {
        RdbStore old = endedEofMarkRdbWithSizeMissing(type);
        AtomicReference<RdbStore> ref = storeRef(store, type);
        RdbStore spyOld = spy(old);
        AtomicReference<RdbStore> switched = new AtomicReference<>();
        doAnswer(inv -> {
            if (switched.get() == null) {
                switched.set(confirmRdb(type, new EofMarkType(randomKeeperRunid())));
            }
            return inv.callRealMethod();
        }).when(spyOld).rdbFileLength();
        assertTrue(ref.compareAndSet(old, spyOld));

        store.flushMeta();
        verify(spyOld, atLeastOnce()).rdbFileLength();
        assertNotNull("switch happened inside the check", switched.get());
        assertEquals(switched.get().getRdbFile().getName(), metaFile(type));
        assertEquals("old rdb length not written onto the new rdb", -1, metaSize(type));
        spyOld.close();
    }

    @Test
    public void testAlignFailureStillFlushesMetaAndRetriesNextRound() throws Exception {
        endedEofMarkRdbWithSizeMissing(RdbStore.Type.NORMAL);

        failMetaWrites.set(true);
        try (LogCapture logs = new LogCapture(DefaultReplicationStore.class)) {
            store.flushMeta();

            List<LogEvent> alignWarns = logs.withTag("[flushMeta][alignRdbSize][fail]");
            assertEquals(1, alignWarns.size());
            assertLogHasReplIdWithoutStack(alignWarns.get(0));
            List<LogEvent> flushWarns = logs.withTag("[flushMeta][fail]");
            assertEquals("meta rewrite still ran after the align failure", 1, flushWarns.size());
            assertLogHasReplIdWithoutStack(flushWarns.get(0));
        } finally {
            failMetaWrites.set(false);
        }
        assertEquals("metaRef unchanged on failure", -1, metaSize(RdbStore.Type.NORMAL));

        store.flushMeta();
        assertEquals(DATA_LEN, metaSize(RdbStore.Type.NORMAL));
    }


    private RdbStore endedEofMarkRdbWithSizeMissing(RdbStore.Type type) throws IOException {
        String eofMark = randomKeeperRunid();
        RdbStore rdb = confirmRdb(type, new EofMarkType(eofMark));
        assertEquals(-1, metaSize(type));
        rdb.writeRdb(Unpooled.wrappedBuffer(randomString(DATA_LEN).getBytes()));
        rdb.writeRdb(Unpooled.wrappedBuffer(eofMark.getBytes()));

        failMetaWrites.set(true);
        try {
            rdb.truncateEndRdb(eofMark.length());
        } finally {
            failMetaWrites.set(false);
        }
        assertFalse(rdb.isWriting());
        assertTrue("rdb is Success in memory", rdb.checkOk());
        assertEquals("onEndRdb size write failed", -1, metaSize(type));
        return rdb;
    }

    private RdbStore confirmRdb(RdbStore.Type type, EofType eofType) throws IOException {
        RdbStore rdb = store.prepareRdb(randomKeeperRunid(), -1, eofType);
        rdb.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
        rdb.updateRdbType(type);
        store.confirmRdb(rdb);
        return rdb;
    }

    private void assertOnlyPeriodicRewrite(RdbStore.Type type) {
        int before = metaWrites.get();
        store.flushMeta();
        assertEquals(type + ": no alignment write", 1, metaWrites.get() - before);
    }

    private void assertLogHasReplIdWithoutStack(LogEvent event) {
        String message = LogCapture.message(event);
        assertTrue(message, message.contains("repl_id=" + getReplId()) && message.contains(META_V2_FILE)
                && message.contains("IOException"));
        assertNull("no stack trace", event.getThrown());
    }

    private long metaSize(RdbStore.Type type) {
        return metaSize(store, type);
    }

    private static long metaSize(DefaultReplicationStore replicationStore, RdbStore.Type type) {
        ReplicationStoreMeta meta = replicationStore.getMetaStore().dupReplicationStoreMeta();
        return type == RdbStore.Type.NORMAL ? meta.getRdbFileSize() : meta.getRordbFileSize();
    }

    private String metaFile(RdbStore.Type type) {
        ReplicationStoreMeta meta = store.getMetaStore().dupReplicationStoreMeta();
        return type == RdbStore.Type.NORMAL ? meta.getRdbFile() : meta.getRordbFile();
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<RdbStore> storeRef(DefaultReplicationStore replicationStore, RdbStore.Type type) {
        String field = type == RdbStore.Type.NORMAL ? "rdbStoreRef" : "rordbStoreRef";
        return (AtomicReference<RdbStore>) ReflectionTestUtils.getField(replicationStore, field);
    }
}
