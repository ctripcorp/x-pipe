package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStore;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class AbstractMetaStoreFlushTest extends AbstractRedisKeeperTest {

    private TailCacheFileSystemTest.FaultyDelegate faulty;
    private TailCacheFileSystem tcf;
    private AsyncFileSystem fs;
    private File baseDir;
    private final String runid = randomKeeperRunid();
    private DefaultMetaStore store;

    @Before
    public void setUpFs() throws IOException {
        TrackingExecutor ioExecutor = new TrackingExecutor(Executors.newCachedThreadPool());
        faulty = new TailCacheFileSystemTest.FaultyDelegate(ioExecutor);
        tcf = spy(new TailCacheFileSystem(faulty, new TailCacheFileSystemConfig(), ioExecutor));
        fs = tcf;
        baseDir = new File(getTestFileDir());
        store = new DefaultMetaStore(baseDir, runid, fs, getReplId());
        store.initialize();
        store.setMasterAddress(new DefaultEndPoint("127.0.0.1", 1000));
    }

    @After
    public void tearDownFs() throws IOException {
        faulty.release();
        store.close();
        fs.shutdown();
    }

    @Override
    protected AsyncFileSystem asyncFileSystem() {
        return fs;
    }

    @Test
    public void testNoFsSaveReachesTfsOnFlush() throws Exception {
        saveUnderNoFs(2000);
        assertEquals("NO_FS write stays in the cache", 1000, portFromFreshFs());

        store.flushMeta();
        assertEquals(2000, portFromFreshFs());
        assertEquals(2000, store.getMasterAddress().getPort());
    }

    @Test
    public void testFlushFailureKeepsMetaRefAndNextFlushWrites() throws Exception {
        saveUnderNoFs(2000);
        ReplicationStoreMeta before = metaRef();
        faulty.failNext(TailCacheFileSystemTest.Op.FILE_WRITE, handle().path, new IOException("flush-eio"));

        try {
            store.flushMeta();
            fail("expected the flush to fail");
        } catch (IOException expected) {
            logger.info("[testFlushFailureKeepsMetaRefAndNextFlushWrites] {}", expected.getMessage());
        }
        assertSame("metaRef unchanged", before, metaRef());
        assertEquals(1000, portFromFreshFs());

        store.flushMeta();
        assertEquals(2000, portFromFreshFs());
    }

    @Test
    public void testFlushWritesCleanEntryWithoutFsync() throws Exception {
        assertFalse(handle().getCacheEntry().isCacheDirty(true));
        int writes = faulty.fileWriteCount;
        int fsyncs = faulty.fileFsyncCount;
        clearInvocations(fs);

        store.flushMeta();
        assertEquals("a clean entry is written again", writes + 1, faulty.fileWriteCount);
        assertEquals(fsyncs, faulty.fileFsyncCount);
        verify(fs, never()).fsync(any(AsyncFile.class));
        assertEquals(1000, portFromFreshFs());
    }

    @Test
    public void testReadOnlyFlushTouchesNoFs() throws Exception {
        store.close();
        DefaultMetaStore reader = new DefaultMetaStore(baseDir, runid, fs, getReplId(), true);
        reader.initialize();
        try {
            clearInvocations(fs);
            reader.flushMeta();
            assertNoWriteNorOpen();
        } finally {
            reader.close();
        }
    }

    @Test
    public void testClosedFlushTouchesNoFs() throws Exception {
        store.close();
        clearInvocations(fs);
        store.flushMeta();
        assertNoWriteNorOpen();
    }

    @Test
    public void testFlushDoesNotOpenMissingHandle() throws Exception {
        fs.close(handle()).get();
        ReflectionTestUtils.setField(store, "metaAsyncFile", null);
        clearInvocations(fs);

        store.flushMeta();
        assertNoWriteNorOpen();
        assertFalse(store.isMetaHandleOpen());
    }

    @Test
    public void testReplicationStoreFlushDelegatesAndOnlyLogsFailure() throws Exception {
        store.close();
        DefaultReplicationStore replicationStore = createDefaultReplicationStore(baseDir, new TestKeeperConfig(),
                runid, createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser());
        MetaStore mockMeta = swapMetaStore(replicationStore);
        try (LogCapture logs = new LogCapture(DefaultReplicationStore.class)) {
            replicationStore.flushMeta();
            verify(mockMeta).flushMeta();

            doThrow(new IOException("flush-eio")).when(mockMeta).flushMeta();
            replicationStore.flushMeta();
            List<LogEvent> warns = logs.withTag("[flushMeta][fail]");
            assertEquals(1, warns.size());
            String message = LogCapture.message(warns.get(0));
            assertTrue(message, message.contains("repl_id=" + getReplId())
                    && message.contains(MetaStore.META_V2_FILE) && message.contains("IOException: flush-eio"));
            assertNull("no stack trace", warns.get(0).getThrown());
        } finally {
            restoreMetaStoreAndClose(replicationStore);
        }
    }

    @Test
    public void testReplicationStoreFlushSkipsClosedAndReadOnly() throws Exception {
        store.close();
        TestKeeperConfig config = new TestKeeperConfig();
        DefaultReplicationStore writable = createDefaultReplicationStore(baseDir, config, runid,
                createkeeperMonitor(), mock(SyncRateManager.class), createRedisOpParser());
        writable.close();
        MetaStore closedMeta = mock(MetaStore.class);
        ReflectionTestUtils.setField(writable, "metaStore", closedMeta);
        writable.flushMeta();
        verify(closedMeta, never()).flushMeta();

        DefaultReplicationStore readOnly = new DefaultReplicationStore(null, baseDir, config, runid,
                new OffsetCommandReaderWriterFactory(), createkeeperMonitor(), mock(SyncRateManager.class),
                createRedisOpParser(), fs, getReplId(), true);
        MetaStore readOnlyMeta = swapMetaStore(readOnly);
        try {
            readOnly.flushMeta();
            verify(readOnlyMeta, never()).flushMeta();
        } finally {
            restoreMetaStoreAndClose(readOnly);
        }
    }


    private final AtomicReference<MetaStore> swapped = new AtomicReference<>();

    private MetaStore swapMetaStore(DefaultReplicationStore replicationStore) {
        swapped.set(replicationStore.getMetaStore());
        MetaStore mockMeta = mock(MetaStore.class);
        ReflectionTestUtils.setField(replicationStore, "metaStore", mockMeta);
        return mockMeta;
    }

    private void restoreMetaStoreAndClose(DefaultReplicationStore replicationStore) throws IOException {
        ReflectionTestUtils.setField(replicationStore, "metaStore", swapped.get());
        replicationStore.close();
    }

    private void saveUnderNoFs(int port) throws IOException {
        tcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.NO_FS);
        store.setMasterAddress(new DefaultEndPoint("127.0.0.1", port));
        tcf.setBackingFsMode(TailCacheFileSystemConfig.BackingFsMode.ASYNC);
    }

    private void assertNoWriteNorOpen() {
        verify(fs, never()).write(any(AsyncFile.class), any(ByteBuf.class));
        verify(fs, never()).open(anyString(), any(AbstractStorageFile.OpenMode.class),
                any(AbstractStorageFile.ReplaceMode.class), anyBoolean(), any());
    }

    private AsyncFile handle() {
        return (AsyncFile) ReflectionTestUtils.getField(store, "metaAsyncFile");
    }

    @SuppressWarnings("unchecked")
    private ReplicationStoreMeta metaRef() {
        return ((AtomicReference<ReplicationStoreMeta>) ReflectionTestUtils.getField(store, "metaRef")).get();
    }

    private int portFromFreshFs() throws Exception {
        AsyncFileSystem fresh = createTestAsyncFileSystem();
        try {
            DefaultMetaStore reader = new DefaultMetaStore(baseDir, runid, fresh, getReplId(), true);
            reader.initialize();
            int port = reader.getMasterAddress().getPort();
            reader.close();
            return port;
        } finally {
            fresh.shutdown();
        }
    }
}
