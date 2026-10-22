package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.DebugFileChannelFactory;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleCloser;
import com.ctrip.xpipe.redis.keeper.storage.FileChannelFactory;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystemConfig;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.*;

/**
 * keeper-tfs-m6 P2 (spec §2.4): the container FS comes with a registered DeferredHandleCloser.
 */
public class ContainerResourceManagerTest {

    @Test
    public void testFsIsCreatedWithCloserAndShutdownStopsBoth() {
        ContainerResourceManager manager = new ContainerResourceManager();
        ReflectionTestUtils.setField(manager, "keeperConfig", new TestKeeperConfig());

        AsyncFileSystem fs = manager.getAsyncFileSystem();
        assertSame("one FS per container", fs, manager.getAsyncFileSystem());
        DeferredHandleCloser closer = DeferredHandleCloser.of(fs);
        assertNotNull("closer registered with the container FS", closer);
        assertSame("exposed for the unclosed-handles API", closer, manager.getDeferredHandleCloser());
        assertNotNull("metric reporter started with the closer",
                ReflectionTestUtils.getField(manager, "unclosedHandleMetricReporter"));
        assertEquals(0, closer.count());

        manager.shutdownAsyncFileSystem();
        assertNull("closer unregistered on shutdown", DeferredHandleCloser.of(fs));
        assertNull(manager.getDeferredHandleCloser());
        assertNull("metric reporter stopped", ReflectionTestUtils.getField(manager, "unclosedHandleMetricReporter"));
        assertNotSame("FS dropped and recreated on demand", fs, manager.getAsyncFileSystem());
        manager.shutdownAsyncFileSystem();
    }

    @Test
    public void testFlushRetryWindowConfig() {
        assertEquals(300_000L, KeeperConfig.DEFAULT_UNCLOSED_HANDLE_FLUSH_RETRY_WINDOW_MILLI);
        assertEquals("keeper.unclosed.handle.flush.retry.window.milli",
                KeeperConfig.KEY_UNCLOSED_HANDLE_FLUSH_RETRY_WINDOW_MILLI);
        TestKeeperConfig config = new TestKeeperConfig();
        assertEquals(300_000L, config.getUnclosedHandleFlushRetryWindowMilli());
        assertEquals(1_000L, config.setUnclosedHandleFlushRetryWindowMilli(1_000L)
                .getUnclosedHandleFlushRetryWindowMilli());
    }

    /** keeper-tfs-m6 P9 (spec §7). */
    @Test
    public void testPerFileCacheLimitInjectedFromConfig() {
        assertEquals("keeper.async.tail.cache.max.size.per.file.bytes",
                KeeperConfig.KEY_ASYNC_TAIL_CACHE_MAX_SIZE_PER_FILE_BYTES);
        assertEquals(100L * 1024 * 1024, KeeperConfig.DEFAULT_ASYNC_TAIL_CACHE_MAX_SIZE_PER_FILE_BYTES);

        TestKeeperConfig config = new TestKeeperConfig();
        assertEquals(100L * 1024 * 1024, config.getAsyncTailCacheMaxSizePerFileBytes());
        TailCacheFileSystem defaultFs = (TailCacheFileSystem) ContainerResourceManager.createAsyncFileSystem(config);
        try {
            assertEquals(100L * 1024 * 1024, defaultFs.getMaxCacheSizePerFileBytes());
        } finally {
            defaultFs.shutdown();
        }

        TailCacheFileSystemConfig defaults = new TailCacheFileSystemConfig();
        config.setAsyncTailCacheMaxSizePerFileBytes(8L * 1024 * 1024);
        TailCacheFileSystem fs = (TailCacheFileSystem) ContainerResourceManager.createAsyncFileSystem(config);
        try {
            assertEquals(8L * 1024 * 1024, fs.getMaxCacheSizePerFileBytes());
            assertEquals("retain chunks untouched", defaults.getMinRetainChunks(), fs.getMinRetainChunks());
            assertEquals("chunk size untouched", defaults.getChunkSize(), fs.getChunkSize());
            assertEquals("global cap still injected", config.getAsyncTailCacheMaxSizeBytes(), fs.getMaxCacheSizeBytes());
        } finally {
            fs.shutdown();
        }
    }

    /** keeper-tfs-m6 §17.3: the factory is chosen once from keeper.fs.debug.enabled. */
    @Test
    public void testChannelFactoryChosenByDebugSwitch() {
        assertSame(FileChannelFactory.DEFAULT, ContainerResourceManager.createChannelFactory(new TestKeeperConfig()));

        long before = refreshThreads();
        FileChannelFactory debug = ContainerResourceManager.createChannelFactory(
                new TestKeeperConfig().setFsDebugEnabled(true));
        try {
            assertTrue(debug instanceof DebugFileChannelFactory);
            assertEquals("injector refresh started", before + 1, refreshThreads());
        } finally {
            debug.close();
        }
        assertTrue("factory close stops the refresh", awaitRefreshThreads(before));
    }

    /** keeper-tfs-m6 §17.3: the FS shutdown closes the injector it was created with. */
    @Test
    public void testFsShutdownClosesInjector() throws Exception {
        long before = refreshThreads();
        AsyncFileSystem fs = ContainerResourceManager.createAsyncFileSystem(
                new TestKeeperConfig().setFsDebugEnabled(true));
        assertEquals(before + 1, refreshThreads());
        fs.shutdown();
        assertTrue("refresh thread stopped by FS shutdown", awaitRefreshThreads(before));
    }

    private static long refreshThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().contains("keeper-fs-fault-refresh")).count();
    }

    private static boolean awaitRefreshThreads(long expected) {
        long deadline = System.currentTimeMillis() + 2000;
        while (refreshThreads() != expected && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return refreshThreads() == expected;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidPerFileCacheLimitFailsFsCreation() {
        // Below minRetainChunks * chunkSize (1MB).
        ContainerResourceManager.createAsyncFileSystem(
                new TestKeeperConfig().setAsyncTailCacheMaxSizePerFileBytes(1024));
    }
}
