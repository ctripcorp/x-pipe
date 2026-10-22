package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.container.ContainerResourceManager;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.*;

/**
 * keeper-tfs-m6 §13.31: faults injected through the debug channel drive the real TailCache paths:
 * meta write failure, EIO recovery reopen, deferred close (C1) and timeout without cancel (C4).
 */
public class DebugFileChannelEndToEndTest {

    private static final ReplId REPL_ID = ReplId.from(1L);
    private static final long WINDOW_MILLIS = 60_000L;

    private Path tempDir;
    private ExecutorService ioExecutor;
    private MutableDebugFileChannelConfig config;
    private FileFaultInjector injector;
    private CountingFactory factory;
    private TailCacheFileSystem tcf;
    private DeferredHandleCloser closer;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("debug-channel-e2e-");
        ioExecutor = Executors.newCachedThreadPool();
        config = new MutableDebugFileChannelConfig();
        // Not started: each test changes config and calls refresh() to apply it at once.
        injector = new FileFaultInjector(config);
        factory = new CountingFactory(injector);
        AsyncTFSBasedFileSystem backing = new AsyncTFSBasedFileSystem(ioExecutor, Long.MAX_VALUE / 2,
                Long.MAX_VALUE / 2_000_000L, factory);
        TailCacheFileSystemConfig cacheConfig = new TailCacheFileSystemConfig()
                .setWriteBatchBytes(1)
                .setIoWaitTimeoutMs(200)
                .setRestoreWaitTimeoutMs(200);
        tcf = new TailCacheFileSystem(backing, cacheConfig, ioExecutor);
        closer = new DeferredHandleCloser(tcf, () -> WINDOW_MILLIS);
        DeferredHandleCloser.register(tcf, closer);
    }

    @After
    public void tearDown() {
        inject(false, 0, "");
        closer.shutdown();
        tcf.shutdown();
        deleteRecursively(tempDir.toFile());
    }

    @Test
    public void testContainerDebugFsOpensDebugChannels() throws Exception {
        AsyncFileSystem fs = ContainerResourceManager.createAsyncFileSystem(
                new TestKeeperConfig().setFsDebugEnabled(true));
        try {
            AsyncFile file = fs.open(tempDir.resolve("container").toString(), AbstractStorageFile.OpenMode.WRITE,
                    AbstractStorageFile.ReplaceMode.NORMAL, false, null).get(5, TimeUnit.SECONDS);
            assertTrue(file.channel instanceof DebugFileChannel);
            fs.close(file).get(5, TimeUnit.SECONDS);
        } finally {
            fs.shutdown();
        }
    }

    @Test
    public void testAtomicMetaWriteEioThrows() throws Exception {
        Path path = tempDir.resolve("meta.v2.json");
        AsyncFile meta = openAtomic(path);
        AsyncFileSystemHelper.writeAllBytes(tcf, meta, new byte[]{1}, "write meta");

        inject(true, 0, "WRITE");
        try {
            AsyncFileSystemHelper.writeAllBytes(tcf, meta, new byte[]{2}, "write meta under EIO");
            fail("expected injected EIO");
        } catch (IOException expected) {
            // m3 FS-R12: the atomic write future carries the backing write result
        }
        assertArrayEquals(new byte[]{1}, Files.readAllBytes(path));

        inject(false, 0, "");
        AsyncFileSystemHelper.writeAllBytes(tcf, meta, new byte[]{3}, "write meta after recovery");
        assertArrayEquals(new byte[]{3}, Files.readAllBytes(path));
        assertTrue(AsyncFileSystemHelper.closeHandle(tcf, meta, REPL_ID, "close meta"));
    }

    @Test
    public void testWriteEioReopenFailsAndCloseIsDeferredUntilRecovery() throws Exception {
        Path path = tempDir.resolve("cmd_0");
        AsyncFile writer = tcf.open(path.toString(), AbstractStorageFile.OpenMode.WRITE,
                AbstractStorageFile.ReplaceMode.NORMAL, false, null).get(5, TimeUnit.SECONDS);
        tcf.write(writer, Unpooled.wrappedBuffer(new byte[]{1, 2})).get(5, TimeUnit.SECONDS);
        waitUntil(() -> fileSize(path) == 2);

        inject(true, 0, "*");
        int opensBefore = factory.opens.get();
        try {
            tcf.write(writer, Unpooled.wrappedBuffer(new byte[]{3, 4, 5})).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // The cached write may or may not report the backing failure; the disk state decides.
        }
        waitUntil(() -> factory.opens.get() > opensBefore);
        assertEquals("EIO recovery reopened through the debug factory and failed", 2, fileSize(path));

        assertFalse("close under EIO is deferred",
                AsyncFileSystemHelper.closeHandle(tcf, writer, REPL_ID, "close cmd_0"));
        assertTrue(closer.isPending(writer));
        assertTrue("dirty tail pinned while pending", tcf.getGlobalCommittedBytes() > 0);

        inject(false, 0, "");
        waitUntil(() -> closer.count() == 0);
        assertTrue(writer.closed);
        assertEquals("deferred close flushed the tail", 5, fileSize(path));
        assertEquals("tracker committed returned", 0, tcf.getGlobalCommittedBytes());
    }

    @Test
    public void testDelayBeyondHelperTimeoutDoesNotCancel() throws Exception {
        Path path = tempDir.resolve("slow.json");
        AsyncFile meta = openAtomic(path);
        AsyncFileSystemHelper.writeAllBytes(tcf, meta, new byte[]{1}, "write slow");

        inject(false, 30_000, "WRITE");
        try {
            AsyncFileSystemHelper.runWithIoTimeout(300,
                    () -> AsyncFileSystemHelper.writeAllBytes(tcf, meta, new byte[]{2}, "write delayed"));
            fail("expected helper timeout");
        } catch (IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("timeout"));
        }
        assertTrue("timed-out IO stays in flight", inFlight().containsKey(meta.ioKey));

        inject(false, 0, "");
        waitUntil(() -> !inFlight().containsKey(meta.ioKey));
        assertArrayEquals("the delayed write still completes", new byte[]{2}, Files.readAllBytes(path));
        assertTrue(AsyncFileSystemHelper.closeHandle(tcf, meta, REPL_ID, "close slow"));
    }

    // ---- helpers ----

    private void inject(boolean eio, long delayMilli, String ops) {
        config.eio = eio;
        config.delayMilli = delayMilli;
        config.ops = ops;
        injector.refresh();
    }

    private AsyncFile openAtomic(Path path) throws Exception {
        return tcf.open(path.toString(), AbstractStorageFile.OpenMode.READ_WRITE,
                AbstractStorageFile.ReplaceMode.ATOMIC, true, null).get(5, TimeUnit.SECONDS);
    }

    private Map<?, ?> inFlight() {
        return (Map<?, ?>) ReflectionTestUtils.getField(tcf, "inFlightIo");
    }

    private static long fileSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            return -1;
        }
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("condition not met within 10s");
            }
            Thread.sleep(20);
        }
    }

    private static void deleteRecursively(File f) {
        File[] children = f.listFiles();
        if (children != null) {
            for (File c : children) {
                deleteRecursively(c);
            }
        }
        f.delete();
    }

    private static final class CountingFactory extends DebugFileChannelFactory {
        final AtomicInteger opens = new AtomicInteger();

        CountingFactory(FileFaultInjector injector) {
            super(injector);
        }

        @Override
        public FileChannel open(Path path, Set<? extends OpenOption> options) throws IOException {
            opens.incrementAndGet();
            return super.open(path, options);
        }
    }
}
