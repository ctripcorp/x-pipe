package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.store.meta.AbstractMetaStore;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStore;
import io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MetaWriteFailureRestoreTest extends AbstractRedisKeeperTest {

    private TrackingExecutor ioExecutor;
    private TailCacheFileSystemTest.FaultyDelegate faulty;
    private AsyncFileSystem fs;
    private File baseDir;
    private Target target;

    @Before
    public void setUpFs() {
        ioExecutor = new TrackingExecutor(Executors.newCachedThreadPool());
        faulty = new TailCacheFileSystemTest.FaultyDelegate(ioExecutor);
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setIoWaitTimeoutMs(200);
        config.setRestoreWaitTimeoutMs(200);
        fs = spy(new TailCacheFileSystem(faulty, config, ioExecutor));
        baseDir = new File(getTestFileDir());
    }

    @After
    public void tearDownFs() throws Exception {
        faulty.release();
        if (target != null) {
            target.close();
        }
        fs.shutdown();
    }


    private void prepareFailureRestoresCommitted(Target t) throws Exception {
        String committed = t.committed();
        AsyncFile handle = t.handle();
        handle.needPrepare = true;
        faulty.failNext(TailCacheFileSystemTest.Op.MKDIR, handle.dirPath,
                new IOException("prepare-1"), new IOException("prepare-2"));

        assertSaveFails(t, null);
        assertEquals("both write attempts failed in prepare", 2, faulty.failureHits.get());
        assertCommittedEverywhere(t, committed);
    }

    private void writeEioRestoresCommitted(Target t, Runnable afterFailure) throws Exception {
        String committed = t.committed();
        faulty.failNext(TailCacheFileSystemTest.Op.FILE_WRITE, t.handle().path, new IOException("write-eio"));

        assertSaveFails(t, "write-eio");
        afterFailure.run();
        assertCommittedEverywhere(t, committed);
    }

    private void timeoutRestoresCacheAndCloseWritesCommitted(Target t) throws Exception {
        String committed = t.committed();
        FileCacheEntry entry = t.handle().getCacheEntry();
        long writtenGenBefore = entry.writtenGen;
        faulty.hangOn(TailCacheFileSystemTest.Op.FILE_WRITE);

        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            try {
                AsyncFileSystemHelper.runWithIoTimeout(300, t::saveNew);
                fail("expected the hung write to time out");
            } catch (IOException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("timeout"));
            }
            assertEquals(1, logs.withTag("[writeAllBytes][rollback-fail]").size());
        }
        assertEquals(committed, t.committed());
        assertEquals(committed, t.readViaHandle());

        faulty.release();
        waitConditionUntilTimeOut(() -> entry.writtenGen != writtenGenBefore, 5000);
        assertTrue("late IO wrote an older generation, entry stays dirty: before=" + writtenGenBefore
                + " written=" + entry.writtenGen + " cache=" + entry.cacheGen + " inconsistent=" + entry.fsInconsistent,
                entry.isCacheDirty(true));

        t.close();
        assertEquals(committed, t.readFromFreshFs());
    }

    private void rewriteFailureKeepsOriginalException(Target t) throws Exception {
        String committed = t.committed();
        faulty.failNext(TailCacheFileSystemTest.Op.FILE_WRITE, t.handle().path,
                new IOException("original-eio"), new IOException("rewrite-eio"));

        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            Throwable thrown = assertSaveFails(t, "original-eio");
            assertFalse(chainContains(thrown, "rewrite-eio"));
            String warn = LogCapture.message(logs.withTag("[writeAllBytes][rollback-fail]").get(0));
            assertTrue(warn, warn.contains("rewrite-eio") && warn.contains(t.handle().path));
            assertTrue("no stack trace", logs.withTag("[writeAllBytes][rollback-fail]").get(0).getThrown() == null);
        }
        assertEquals(2, faulty.failureHits.get());
        assertEquals(committed, t.committed());
        assertEquals("the failed rewrite still swapped the cache back", committed, t.readViaHandle());
    }

    private void openFailureSkipsRewrite(Target t) throws Exception {
        String committed = t.committed();
        AsyncFile handle = t.handle();
        fs.close(handle).get();
        ReflectionTestUtils.setField(t.owner(), t.handleField(), null);
        doReturn(CompletableFuture.failedFuture(new IOException("open-fail"))).when(fs)
                .open(anyString(), any(AbstractStorageFile.OpenMode.class), any(AbstractStorageFile.ReplaceMode.class),
                        anyBoolean(), any());
        clearInvocations(fs);

        assertSaveFails(t, "open-fail");
        verify(fs, never()).write(any(AsyncFile.class), any(ByteBuf.class));
        assertEquals(committed, t.committed());
    }


    @Test
    public void testMetaPrepareFailureRestoresCommitted() throws Exception {
        prepareFailureRestoresCommitted(metaTarget());
    }

    @Test
    public void testMetaWriteEioRestoresCommitted() throws Exception {
        writeEioRestoresCommitted(metaTarget(), () -> { });
    }

    @Test
    public void testMetaTimeoutRestoresCacheAndCloseWritesCommitted() throws Exception {
        timeoutRestoresCacheAndCloseWritesCommitted(metaTarget());
    }

    @Test
    public void testMetaRewriteFailureKeepsOriginalException() throws Exception {
        rewriteFailureKeepsOriginalException(metaTarget());
    }

    @Test
    public void testMetaOpenFailureSkipsRewrite() throws Exception {
        openFailureSkipsRewrite(metaTarget());
    }


    @Test
    public void testManagerPrepareFailureRestoresCommitted() throws Exception {
        prepareFailureRestoresCommitted(managerTarget());
    }

    @Test
    public void testManagerWriteEioKeepsCurrentMetaAndStore() throws Exception {
        ManagerTarget t = managerTarget();
        ReplicationStore published = t.manager.getOpenedStore();
        assertNotNull(published);
        writeEioRestoresCommitted(t, () -> assertSame("recordLatestStore failure keeps the old store",
                published, t.manager.getOpenedStore()));
    }

    @Test
    public void testManagerTimeoutRestoresCacheAndCloseWritesCommitted() throws Exception {
        timeoutRestoresCacheAndCloseWritesCommitted(managerTarget());
    }

    @Test
    public void testManagerRewriteFailureKeepsOriginalException() throws Exception {
        rewriteFailureKeepsOriginalException(managerTarget());
    }

    @Test
    public void testManagerOpenFailureSkipsRewrite() throws Exception {
        openFailureSkipsRewrite(managerTarget());
    }


    private Throwable assertSaveFails(Target t, String causeText) {
        try {
            t.saveNew();
        } catch (Throwable th) {
            assertTrue("unexpected failure: " + th, causeText == null || chainContains(th, causeText));
            return th;
        }
        throw new AssertionError("expected the save to fail");
    }

    private void assertCommittedEverywhere(Target t, String committed) throws Exception {
        assertEquals("memory unchanged", committed, t.committed());
        assertEquals("cache holds the committed content", committed, t.readViaHandle());
        t.close();
        assertEquals("TFS holds the committed content", committed, t.readFromFreshFs());
    }

    private static boolean chainContains(Throwable th, String text) {
        for (Throwable c = th; c != null; c = c.getCause()) {
            if (String.valueOf(c.getMessage()).contains(text)) {
                return true;
            }
        }
        return false;
    }

    private byte[] readViaHandle(AsyncFile handle) throws IOException {
        long size = AsyncFileSystemHelper.await(() -> fs.size(handle), "size");
        return AsyncFileSystemHelper.readAllBytes(fs, handle, size, 0, "read");
    }

    private interface Target {
        AsyncFile handle();

        Object owner();

        String handleField();

        void saveNew() throws IOException;

        String committed();

        String readViaHandle() throws IOException;

        void close() throws Exception;

        String readFromFreshFs() throws Exception;
    }

    private MetaTarget metaTarget() throws IOException {
        MetaTarget t = new MetaTarget();
        target = t;
        return t;
    }

    private ManagerTarget managerTarget() throws Exception {
        ManagerTarget t = new ManagerTarget();
        target = t;
        return t;
    }

    private class MetaTarget implements Target {
        private final String runid = randomKeeperRunid();
        private final DefaultMetaStore store;

        MetaTarget() throws IOException {
            store = new DefaultMetaStore(baseDir, runid, fs, getReplId());
            store.initialize();
            store.setMasterAddress(new DefaultEndPoint("127.0.0.1", 1000));
        }

        public AsyncFile handle() {
            return (AsyncFile) ReflectionTestUtils.getField(store, handleField());
        }

        public Object owner() {
            return store;
        }

        public String handleField() {
            return "metaAsyncFile";
        }

        public void saveNew() throws IOException {
            store.setMasterAddress(new DefaultEndPoint("127.0.0.1", 2000));
        }

        public String committed() {
            return String.valueOf(store.getMasterAddress().getPort());
        }

        public String readViaHandle() throws IOException {
            String json = new String(MetaWriteFailureRestoreTest.this.readViaHandle(handle()));
            return String.valueOf(AbstractMetaStore.deserializeFromStringV2(json).getMasterAddress().getPort());
        }

        public void close() throws IOException {
            store.close();
        }

        public String readFromFreshFs() throws Exception {
            AsyncFileSystem fresh = createTestAsyncFileSystem();
            try {
                DefaultMetaStore reader = new DefaultMetaStore(baseDir, runid, fresh, getReplId(), true);
                reader.initialize();
                String port = String.valueOf(reader.getMasterAddress().getPort());
                reader.close();
                return port;
            } finally {
                fresh.shutdown();
            }
        }
    }

    private class ManagerTarget implements Target {
        private final DefaultReplicationStoreManager manager;

        ManagerTarget() throws Exception {
            manager = newManager(fs, true);
            manager.create();
        }

        public AsyncFile handle() {
            return (AsyncFile) ReflectionTestUtils.getField(manager, handleField());
        }

        public Object owner() {
            return manager;
        }

        public String handleField() {
            return "managerMetaAsyncFile";
        }

        public void saveNew() throws IOException {
            manager.create();
        }

        public String committed() {
            return manager.getCurrentMetaCache().getProperty("latest.store.dir");
        }

        public String readViaHandle() throws IOException {
            Properties meta = new Properties();
            meta.load(new ByteArrayInputStream(MetaWriteFailureRestoreTest.this.readViaHandle(handle())));
            return meta.getProperty("latest.store.dir");
        }

        public void close() throws Exception {
            LifecycleHelper.stopIfPossible(manager);
            LifecycleHelper.disposeIfPossible(manager);
        }

        public String readFromFreshFs() throws Exception {
            AsyncFileSystem fresh = createTestAsyncFileSystem();
            DefaultReplicationStoreManager reader = newManager(fresh, false);
            try {
                return reader.reloadLatestStoreDir();
            } finally {
                LifecycleHelper.disposeIfPossible(reader);
                fresh.shutdown();
            }
        }
    }

    private DefaultReplicationStoreManager newManager(AsyncFileSystem fileSystem, boolean start) throws Exception {
        TestKeeperConfig config = new TestKeeperConfig();
        config.setReplicationStoreGcIntervalSeconds(3600);
        DefaultReplicationStoreManager manager = new DefaultReplicationStoreManager(config, getReplId(),
                randomKeeperRunid(), baseDir, createkeeperMonitor(), mock(SyncRateManager.class),
                createRedisOpParser(), null, fileSystem) {
            @Override
            protected ReplicationStore createReplicationStore(File storeBaseDir, KeeperConfig keeperConfig,
                    String keeperRunid, KeeperMonitor keeperMonitor, SyncRateManager syncRateManager) {
                ReplicationStore store = mock(ReplicationStore.class);
                when(store.checkOk()).thenReturn(true);
                return store;
            }
        };
        LifecycleHelper.initializeIfPossible(manager);
        if (start) {
            LifecycleHelper.startIfPossible(manager);
        }
        return manager;
    }
}
