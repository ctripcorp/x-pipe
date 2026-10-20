package com.ctrip.xpipe.redis.keeper.storage;

import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AsyncFileSystemHelperTimeoutNoCancelTest {

    private static final long TIMEOUT_MILLIS = 50L;

    private Path tempDir;
    private TailCacheFileSystemTest.FaultyDelegate faulty;
    private TailCacheFileSystem tcf;

    @After
    public void tearDown() {
        if (faulty != null) {
            faulty.release();
        }
        if (tcf != null) {
            tcf.shutdown();
        }
        if (tempDir != null) {
            deleteRecursively(tempDir.toFile());
        }
    }

    @Test
    public void testTimeoutAndInterruptDoNotCancel() throws Exception {
        CompletableFuture<Long> timedOut = new CompletableFuture<>();
        try {
            AsyncFileSystemHelper.runWithIoTimeout(TIMEOUT_MILLIS,
                    () -> AsyncFileSystemHelper.await(() -> timedOut, "slow truncate"));
            fail("expected timeout");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("timeout"));
        }
        assertFalse(timedOut.isCancelled());

        CompletableFuture<Void> explicitTimeout = new CompletableFuture<>();
        try {
            AsyncFileSystemHelper.await(explicitTimeout, "slow fsync", TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            fail("expected timeout");
        } catch (IOException expected) {
        }
        assertFalse(explicitTimeout.isCancelled());

        CompletableFuture<Boolean> interrupted = new CompletableFuture<>();
        Thread.currentThread().interrupt();
        try {
            AsyncFileSystemHelper.await(interrupted, "interrupted mkdir");
            fail("expected interrupt");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("interrupted"));
        } finally {
            assertTrue("interrupt flag restored", Thread.interrupted());
        }
        assertFalse(interrupted.isCancelled());
    }

    @Test
    public void testLateFailureLoggedOnceWithoutStackAndLateSuccessSilent() throws Exception {
        CompletableFuture<Void> lateFail = new CompletableFuture<>();
        CompletableFuture<Void> lateOk = new CompletableFuture<>();
        try (LogCapture logs = new LogCapture(AsyncFileSystemHelper.class)) {
            awaitAndTimeOut(lateFail, "delete late-fail");
            awaitAndTimeOut(lateOk, "delete late-ok");

            lateFail.completeExceptionally(new IOException("late EIO"));
            lateOk.complete(null);

            List<LogEvent> lateLogs = logs.withTag("[await][late-fail]");
            assertEquals(1, lateLogs.size());
            String message = LogCapture.message(lateLogs.get(0));
            assertTrue(message, message.contains("delete late-fail") && message.contains("IOException: late EIO"));
            assertNull("no stack trace", lateLogs.get(0).getThrown());
            assertTrue("late success is not logged", logs.withTag("delete late-ok").isEmpty());
        }
    }

    @Test
    public void testTimedOutAtomicWriteStaysInFlightAndNextWriteWaits() throws Exception {
        tempDir = Files.createTempDirectory("helper-no-cancel-");
        TrackingExecutor ioExecutor = new TrackingExecutor(Executors.newCachedThreadPool());
        faulty = new TailCacheFileSystemTest.FaultyDelegate(ioExecutor);
        TailCacheFileSystemConfig config = new TailCacheFileSystemConfig();
        config.setIoWaitTimeoutMs(200);
        config.setRestoreWaitTimeoutMs(200);
        tcf = new TailCacheFileSystem(faulty, config, ioExecutor);
        String path = tempDir.resolve("meta.v2.json").toString();
        AsyncFile file = tcf.open(path, AbstractStorageFile.OpenMode.READ_WRITE,
                AbstractStorageFile.ReplaceMode.ATOMIC, true, null).get(5, TimeUnit.SECONDS);
        AsyncFileSystemHelper.writeAllBytes(tcf, file, new byte[]{1}, "write committed");

        faulty.hangOn(TailCacheFileSystemTest.Op.FILE_WRITE);
        int writesBefore = faulty.fileWriteCount;
        try {
            AsyncFileSystemHelper.runWithIoTimeout(300,
                    () -> AsyncFileSystemHelper.writeAllBytes(tcf, file, new byte[]{2}, "write hung"));
            fail("expected timeout");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("timeout"));
        }
        assertTrue("timed-out IO still registered in-flight", inFlight().containsKey(file.ioKey));

        try {
            AsyncFileSystemHelper.writeAllBytes(tcf, file, new byte[]{3}, "write after timeout");
            fail("expected the in-flight wait to time out");
        } catch (OperationNotExecutedException expected) {
        }

        faulty.release();
        long deadline = System.currentTimeMillis() + 5000;
        while (inFlight().containsKey(file.ioKey) && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertFalse(inFlight().containsKey(file.ioKey));
        assertEquals("only the hung write reached the delegate", writesBefore + 1, faulty.fileWriteCount);

        tcf.close(file).get(5, TimeUnit.SECONDS);
        assertArrayEquals("close flushes the last cached content", new byte[]{3}, Files.readAllBytes(tempDir.resolve("meta.v2.json")));
    }

    private Map<?, ?> inFlight() {
        return (Map<?, ?>) org.springframework.test.util.ReflectionTestUtils.getField(tcf, "inFlightIo");
    }

    private static void awaitAndTimeOut(CompletableFuture<?> future, String operation) {
        try {
            AsyncFileSystemHelper.runWithIoTimeout(TIMEOUT_MILLIS, () -> AsyncFileSystemHelper.await(future, operation));
            fail("expected timeout");
        } catch (IOException expected) {
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
}
