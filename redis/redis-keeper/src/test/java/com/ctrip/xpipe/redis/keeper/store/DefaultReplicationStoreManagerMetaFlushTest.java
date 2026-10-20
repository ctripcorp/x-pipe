package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.LogCapture;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.*;

public class DefaultReplicationStoreManagerMetaFlushTest extends AbstractRedisKeeperTest {

    private static final long CHECK_PERIOD_MILLI = 20;
    private static final String LATEST_STORE_DIR = "latest.store.dir";

    private AsyncFileSystem fs;
    private File baseDir;
    private TestKeeperConfig config;
    private final List<ReplicationStore> created = new CopyOnWriteArrayList<>();
    private boolean realStores;
    private DefaultReplicationStoreManager manager;

    @Before
    public void setUpFs() {
        fs = spy(createTestAsyncFileSystem());
        baseDir = new File(getTestFileDir());
        config = new TestKeeperConfig();
        config.setReplicationStoreGcIntervalSeconds(3600);
        config.setMetaFlushIntervalMilli(TimeUnit.HOURS.toMillis(1));
    }

    @After
    public void tearDownFs() throws Exception {
        if (manager != null) {
            LifecycleHelper.stopIfPossible(manager);
            LifecycleHelper.disposeIfPossible(manager);
        }
        fs.shutdown();
    }

    @Test
    public void testScheduledByIntervalAndPicksUpChangedInterval() throws Exception {
        config.setMetaFlushIntervalMilli(300);
        long startAt = System.currentTimeMillis();
        ReplicationStore store = startWithStore();
        AtomicLong firstAt = new AtomicLong();
        doAnswer(inv -> firstAt.compareAndSet(0, System.currentTimeMillis())).when(store).flushMeta();

        verify(store, timeout(3000).atLeast(2)).flushMeta();
        assertTrue("first flush after one interval", firstAt.get() - startAt >= 300);

        config.setMetaFlushIntervalMilli(TimeUnit.HOURS.toMillis(1));
        sleep(100);
        clearInvocations(store);
        sleep(300);
        verify(store, never()).flushMeta();

        config.setMetaFlushIntervalMilli(50);
        verify(store, timeout(1000).atLeastOnce()).flushMeta();
    }

    @Test
    public void testNonPositiveIntervalDisablesUntilRestored() throws Exception {
        config.setMetaFlushIntervalMilli(0);
        ReplicationStore store = startWithStore();
        AsyncFile handle = managerHandle();
        clearInvocations(fs);
        sleep(200);
        config.setMetaFlushIntervalMilli(-1);
        sleep(100);
        verify(store, never()).flushMeta();
        verify(fs, never()).write(same(handle), any(ByteBuf.class));

        config.setMetaFlushIntervalMilli(50);
        verify(store, timeout(1000).atLeastOnce()).flushMeta();
        verify(fs, atLeastOnce()).write(same(handle), any(ByteBuf.class));
    }

    @Test
    public void testReadOnlyStartDoesNotSchedule() throws Exception {
        config.setMetaFlushIntervalMilli(10);
        manager = newManager(config);
        manager.setReadOnly(true);
        LifecycleHelper.startIfPossible(manager);
        assertNull(metaFlushFuture());
        sleep(100);
        verify(fs, never()).write(any(AsyncFile.class), any(ByteBuf.class));
    }

    @Test
    public void testStopAndDisposeCancelAndRestartReschedules() throws Exception {
        config.setMetaFlushIntervalMilli(50);
        ReplicationStore first = startWithStore();
        ScheduledFuture<?> firstFuture = metaFlushFuture();
        verify(first, timeout(1000).atLeastOnce()).flushMeta();

        LifecycleHelper.stopIfPossible(manager);
        assertTrue(firstFuture.isCancelled());
        assertNull(metaFlushFuture());

        LifecycleHelper.startIfPossible(manager);
        ScheduledFuture<?> secondFuture = metaFlushFuture();
        assertNotNull(secondFuture);
        assertNotSame(firstFuture, secondFuture);
        ReplicationStore reopened = manager.getCurrent();
        assertNotSame(first, reopened);
        verify(reopened, timeout(1000).atLeastOnce()).flushMeta();

        LifecycleHelper.stopIfPossible(manager);
        LifecycleHelper.disposeIfPossible(manager);
        assertTrue(secondFuture.isCancelled());
        assertNull(metaFlushFuture());
        clearInvocations(reopened);
        sleep(150);
        verify(reopened, never()).flushMeta();
    }

    @Test
    public void testFlushWritesCurrentStoreDir() throws Exception {
        startWithStore();
        String dir = latestDir();
        writeViaHandle("unpublished");
        Properties diverged = new Properties();
        diverged.setProperty(LATEST_STORE_DIR, "diverged");
        diverged.setProperty("extra", "kept");
        currentMetaRef().set(diverged);

        manager.flushMetaPeriodically();
        assertEquals(dir, readViaHandle());
        assertEquals(dir, latestDir());
        assertEquals("kept", manager.getCurrentMetaCache().getProperty("extra"));
    }

    @Test
    public void testFlushRestoresDirAfterRecordLatestStoreFailure() throws Exception {
        ReplicationStore published = startWithStore();
        String dir = latestDir();
        AtomicInteger calls = new AtomicInteger();
        doAnswer(inv -> {
            int n = calls.incrementAndGet();
            if (n == 1) {
                ((CompletableFuture<?>) inv.callRealMethod()).join();
                return CompletableFuture.failedFuture(new IOException("eio-after-write"));
            }
            if (n == 2) {
                return CompletableFuture.failedFuture(new IOException("rollback-eio"));
            }
            return inv.callRealMethod();
        }).when(fs).write(same(managerHandle()), any(ByteBuf.class));

        try {
            manager.create();
            fail("expected recordLatestStore to fail");
        } catch (IOException expected) {
            logger.info("[testFlushRestoresDirAfterRecordLatestStoreFailure] {}", expected.getMessage());
        }
        assertSame(published, manager.getOpenedStore());
        assertEquals(dir, latestDir());
        assertNotEquals("cache and TFS hold the unpublished dir", dir, readViaHandle());

        config.setMinTimeMilliToGcAfterCreate(0);
        sleep(5);
        AsyncFile handle = managerHandle();
        clearInvocations(fs);
        manager.gc();
        verify(fs, never()).read(same(handle), anyLong(), anyLong());
        assertEquals(dir, latestDir());
        assertTrue("gc keeps the published store dir", new File(manager.getBaseDir(), dir).isDirectory());

        manager.flushMetaPeriodically();
        assertEquals(dir, readViaHandle());
        LifecycleHelper.stopIfPossible(manager);
        assertEquals(dir, readFromFreshFs());
    }

    @Test
    public void testFlushDoesNotOpenMissingHandle() throws Exception {
        ReplicationStore store = startWithStore();
        fs.close(managerHandle()).get();
        ReflectionTestUtils.setField(manager, "managerMetaAsyncFile", null);
        clearInvocations(fs);

        manager.flushMetaPeriodically();
        assertNoWriteNorOpen();
        assertFalse(manager.isManagerMetaHandleOpen());
        verify(store).flushMeta();
    }

    @Test
    public void testNoCurrentStoreWritesNothing() throws Exception {
        ReplicationStore store = startWithStore();
        when(store.checkOk()).thenReturn(false);
        clearInvocations(fs);
        manager.flushMetaPeriodically();
        assertNoWriteNorOpen();
        verify(store, never()).flushMeta();

        manager.releaseCurrentStore();
        clearInvocations(fs);
        manager.flushMetaPeriodically();
        assertNoWriteNorOpen();
        verify(store, never()).flushMeta();
    }

    @Test
    public void testManagerMetaFailureStillFlushesStoreAndLogsWithoutStack() throws Exception {
        ReplicationStore store = startWithStore();
        doReturn(CompletableFuture.failedFuture(new IOException("flush-eio")))
                .when(fs).write(same(managerHandle()), any(ByteBuf.class));
        String path = new File(manager.getBaseDir(), "store_manager_meta.properties").getAbsolutePath();

        try (LogCapture logs = new LogCapture(manager.getClass())) {
            manager.flushMetaPeriodically();
            List<LogEvent> warns = logs.withTag("[flushManagerMeta][fail]");
            assertEquals(1, warns.size());
            String message = LogCapture.message(warns.get(0));
            assertTrue(message, message.contains("repl_id=" + getReplId()) && message.contains(path)
                    && message.contains("IOException"));
            assertNull("no stack trace", warns.get(0).getThrown());
        }
        verify(store).flushMeta();
    }

    @Test
    public void testThrowableDoesNotStopSchedule() throws Exception {
        TestKeeperConfig spyConfig = spy(config);
        doThrow(new RuntimeException("config-boom")).doReturn(50L).when(spyConfig).getMetaFlushIntervalMilli();
        manager = newManager(spyConfig);
        LifecycleHelper.startIfPossible(manager);
        ReplicationStore store = manager.create();
        doThrow(new AssertionError("store-boom")).doNothing().when(store).flushMeta();

        verify(store, timeout(2000).atLeast(2)).flushMeta();
        verify(spyConfig, atLeast(3)).getMetaFlushIntervalMilli();
    }

    @Test
    public void testStopWaitsForTickInManagerMetaWrite() throws Exception {
        startWithStore();
        AsyncFile handle = managerHandle();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<String> events = new CopyOnWriteArrayList<>();
        doAnswer(inv -> {
            entered.countDown();
            Uninterruptibles.awaitUninterruptibly(release);
            Object result = inv.callRealMethod();
            events.add("write-done");
            return result;
        }).when(fs).write(same(handle), any(ByteBuf.class));
        doAnswer(inv -> {
            events.add("close");
            return inv.callRealMethod();
        }).when(fs).close(same(handle));

        Thread tick = new Thread(manager::flushMetaPeriodically);
        tick.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Thread stopper = new Thread(() -> {
            try {
                LifecycleHelper.stopIfPossible(manager);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        stopper.start();
        waitConditionUntilTimeOut(() -> stopper.getState() == Thread.State.BLOCKED, 2000);
        assertTrue(events.isEmpty());

        release.countDown();
        tick.join(5000);
        stopper.join(5000);
        assertFalse(tick.isAlive() || stopper.isAlive());
        assertEquals(Arrays.asList("write-done", "close"), events);

        clearInvocations(fs);
        manager.flushMetaPeriodically();
        assertNoWriteNorOpen();
    }

    @Test
    public void testStoreCloseWaitsForTickInStoreMetaWrite() throws Exception {
        realStores = true;
        DefaultReplicationStore store = (DefaultReplicationStore) startWithStore();
        AsyncFile metaHandle = (AsyncFile) ReflectionTestUtils.getField(store.getMetaStore(), "metaAsyncFile");
        assertNotNull(metaHandle);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(inv -> {
            entered.countDown();
            Uninterruptibles.awaitUninterruptibly(release);
            return inv.callRealMethod();
        }).when(fs).write(same(metaHandle), any(ByteBuf.class));

        Thread tick = new Thread(manager::flushMetaPeriodically);
        tick.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Thread releaser = new Thread(() -> {
            try {
                manager.releaseCurrentStore();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        releaser.start();
        sleep(200);
        assertTrue("store close waits for the in-lock flush", releaser.isAlive());

        release.countDown();
        tick.join(5000);
        releaser.join(5000);
        assertFalse(tick.isAlive() || releaser.isAlive());
        assertFalse(store.checkOk());

        clearInvocations(fs);
        manager.flushMetaPeriodically();
        store.flushMeta();
        assertNoWriteNorOpen();
    }

    @Test
    public void testTickWaitingOutsideLockSkipsAfterSwitchToReadOnly() throws Exception {
        startWithStore();
        Thread tick = new Thread(manager::flushMetaPeriodically);
        synchronized (manager) {
            tick.start();
            waitConditionUntilTimeOut(() -> tick.getState() == Thread.State.BLOCKED, 2000);
            LifecycleHelper.stopIfPossible(manager);
            manager.setReadOnly(true);
            LifecycleHelper.startIfPossible(manager);
            assertNotNull("read-only store and meta handle opened", manager.getCurrent());
            clearInvocations(fs);
        }
        tick.join(5000);
        assertFalse(tick.isAlive());
        assertTrue(manager.isReadOnly() && manager.getLifecycleState().isStarted());
        assertNoWriteNorOpen();
        verify(manager.getOpenedStore(), never()).flushMeta();
    }

    @Test
    public void testTickSkipsAfterDestroyWithoutStop() throws Exception {
        ReplicationStore store = startWithStore();
        manager.destroy();
        clearInvocations(fs);
        manager.flushMetaPeriodically();
        assertNoWriteNorOpen();
        verify(store, never()).flushMeta();
    }

    @Test
    public void testCreateReplacesStoreThenFlushesNewOnly() throws Exception {
        ReplicationStore old = startWithStore();
        ReplicationStore replaced = manager.create();
        assertNotSame(old, replaced);
        String newDir = latestDir();
        writeViaHandle("stale");

        manager.flushMetaPeriodically();
        verify(replaced).flushMeta();
        verify(old, never()).flushMeta();
        assertEquals(newDir, readViaHandle());
    }


    private ReplicationStore startWithStore() throws Exception {
        manager = newManager(config);
        LifecycleHelper.startIfPossible(manager);
        return manager.create();
    }

    private DefaultReplicationStoreManager newManager(KeeperConfig keeperConfig) throws Exception {
        return newManager(keeperConfig, fs);
    }

    private DefaultReplicationStoreManager newManager(KeeperConfig keeperConfig, AsyncFileSystem fileSystem) throws Exception {
        DefaultReplicationStoreManager m = new DefaultReplicationStoreManager(keeperConfig, getReplId(),
                randomKeeperRunid(), baseDir, createkeeperMonitor(), mock(SyncRateManager.class),
                createRedisOpParser(), null, fileSystem) {
            @Override
            protected ReplicationStore createReplicationStore(File storeBaseDir, KeeperConfig kc, String runid,
                    KeeperMonitor monitor, SyncRateManager rateManager) throws IOException {
                ReplicationStore store;
                if (realStores) {
                    store = super.createReplicationStore(storeBaseDir, kc, runid, monitor, rateManager);
                } else {
                    store = mock(ReplicationStore.class);
                    when(store.checkOk()).thenReturn(true);
                }
                created.add(store);
                return store;
            }
        };
        m.setMetaFlushCheckPeriodMilli(CHECK_PERIOD_MILLI);
        LifecycleHelper.initializeIfPossible(m);
        return m;
    }

    private ScheduledFuture<?> metaFlushFuture() {
        return (ScheduledFuture<?>) ReflectionTestUtils.getField(manager, "metaFlushFuture");
    }

    private AsyncFile managerHandle() {
        AsyncFile handle = (AsyncFile) ReflectionTestUtils.getField(manager, "managerMetaAsyncFile");
        assertNotNull(handle);
        return handle;
    }

    @SuppressWarnings("unchecked")
    private AtomicReference<Properties> currentMetaRef() {
        return (AtomicReference<Properties>) ReflectionTestUtils.getField(manager, "currentMeta");
    }

    private String latestDir() {
        return manager.getCurrentMetaCache().getProperty(LATEST_STORE_DIR);
    }

    private void writeViaHandle(String dir) throws IOException {
        Properties meta = new Properties();
        meta.setProperty(LATEST_STORE_DIR, dir);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        meta.store(out, null);
        AsyncFileSystemHelper.writeAllBytes(fs, managerHandle(), out.toByteArray(), "test write manager meta");
    }

    private String readViaHandle() throws IOException {
        AsyncFile handle = managerHandle();
        long size = AsyncFileSystemHelper.await(() -> fs.size(handle), "size");
        Properties meta = new Properties();
        meta.load(new ByteArrayInputStream(AsyncFileSystemHelper.readAllBytes(fs, handle, size, 0, "read")));
        return meta.getProperty(LATEST_STORE_DIR);
    }

    private String readFromFreshFs() throws Exception {
        AsyncFileSystem fresh = createTestAsyncFileSystem();
        DefaultReplicationStoreManager reader = newManager(config, fresh);
        try {
            return reader.reloadLatestStoreDir();
        } finally {
            LifecycleHelper.disposeIfPossible(reader);
            fresh.shutdown();
        }
    }

    private void assertNoWriteNorOpen() {
        verify(fs, never()).write(any(AsyncFile.class), any(ByteBuf.class));
        verify(fs, never()).open(anyString(), any(AbstractStorageFile.OpenMode.class),
                any(AbstractStorageFile.ReplaceMode.class), anyBoolean(), any());
    }
}
