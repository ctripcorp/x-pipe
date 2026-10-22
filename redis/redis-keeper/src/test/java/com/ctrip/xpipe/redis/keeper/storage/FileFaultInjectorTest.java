package com.ctrip.xpipe.redis.keeper.storage;

import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.xpipe.redis.keeper.storage.FileFaultInjector.Category;

import static org.junit.Assert.*;

public class FileFaultInjectorTest {

    private static final String PATH = "/data/keeper/replA/cmd_0";
    private static final String OTHER_PATH = "/data/keeper/replB/cmd_0";

    private MutableDebugFileChannelConfig config;
    private CountingConfig counting;
    private FileFaultInjector injector;
    private LogCapture logs;

    // The injector is not started: tests change config and call refresh() to swap the snapshot,
    // the refresh thread itself is covered by testStartedInjectorRefreshesUntilClosed.
    @Before
    public void setUp() {
        config = new MutableDebugFileChannelConfig();
        counting = new CountingConfig(config);
        logs = new LogCapture(FileFaultInjector.class);
        injector = new FileFaultInjector(counting);
    }

    @After
    public void tearDown() {
        injector.close();
        logs.close();
    }

    @Test
    public void testNoInjectionByDefault() throws Exception {
        long start = System.nanoTime();
        for (Category category : Category.values()) {
            injector.inject(category, PATH);
        }
        assertTrue(elapsedMillis(start) < FileFaultInjector.SLEEP_SLICE_MILLI);
    }

    @Test
    public void testInjectNeverReadsConfig() throws Exception {
        config.delayMilli = 1;
        injector.refresh();
        int reads = counting.reads.get();
        for (int i = 0; i < 20; i++) {
            injector.inject(Category.WRITE, PATH);
        }
        assertEquals(reads, counting.reads.get());

        config.eio = true;
        injector.inject(Category.WRITE, PATH);
        injector.refresh();
        assertThrowsIo(Category.WRITE, PATH);
    }

    @Test
    public void testEioIsRecognizedAsEio() {
        update(() -> config.eio = true);
        for (Category category : Category.values()) {
            IOException e = assertThrowsIo(category, PATH);
            assertEquals(FileFaultInjector.EIO_MESSAGE, e.getMessage());
            assertTrue(StorageUtil.wrapIOException(e) instanceof EIOException);
        }
    }

    @Test
    public void testOpsFilter() throws Exception {
        update(() -> {
            config.eio = true;
            config.ops = " Read, bogus ,FORCE ";
        });
        assertThrowsIo(Category.READ, PATH);
        assertThrowsIo(Category.FORCE, PATH);
        injector.inject(Category.OPEN, PATH);
        injector.inject(Category.WRITE, PATH);

        update(() -> config.ops = "read, * ");
        for (Category category : Category.values()) {
            assertThrowsIo(category, PATH);
        }

        for (String none : new String[]{"", "  ", null}) {
            update(() -> config.ops = none);
            for (Category category : Category.values()) {
                injector.inject(category, PATH);
            }
        }
    }

    @Test
    public void testParseOps() {
        assertEquals(EnumSet.allOf(Category.class), FileFaultInjector.parseOps("*"));
        assertEquals(EnumSet.of(Category.OPEN, Category.WRITE), FileFaultInjector.parseOps("open,Write,x"));
        assertEquals(EnumSet.noneOf(Category.class), FileFaultInjector.parseOps(""));
        assertEquals(EnumSet.noneOf(Category.class), FileFaultInjector.parseOps(null));
    }

    @Test
    public void testPathFilter() throws Exception {
        update(() -> {
            config.eio = true;
            config.pathContains = "/replA/";
        });
        assertThrowsIo(Category.WRITE, PATH);
        injector.inject(Category.WRITE, OTHER_PATH);
        injector.inject(Category.WRITE, null);

        update(() -> config.pathContains = "");
        assertThrowsIo(Category.WRITE, OTHER_PATH);
    }

    @Test
    public void testDelay() throws Exception {
        update(() -> config.delayMilli = 250);
        long start = System.nanoTime();
        injector.inject(Category.WRITE, PATH);
        assertTrue(elapsedMillis(start) >= 250);

        update(() -> config.pathContains = "/replA/");
        start = System.nanoTime();
        injector.inject(Category.WRITE, OTHER_PATH);
        assertTrue(elapsedMillis(start) < FileFaultInjector.SLEEP_SLICE_MILLI);
    }

    @Test
    public void testDelayThenEio() {
        update(() -> {
            config.delayMilli = 200;
            config.eio = true;
        });
        long start = System.nanoTime();
        assertThrowsIo(Category.READ, PATH);
        assertTrue(elapsedMillis(start) >= 200);
    }

    @Test
    public void testLoweringDelayReleasesWaitingOperation() throws Exception {
        assertReleasedWithinSlice(() -> config.delayMilli = 0);
    }

    @Test
    public void testRemovingCategoryReleasesWaitingOperationWithoutEio() throws Exception {
        config.eio = true;
        assertReleasedWithinSlice(() -> config.ops = "OPEN");
    }

    @Test
    public void testInterruptEndsDelayAndKeepsFlag() throws Exception {
        update(() -> config.delayMilli = 30_000);
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread worker = new Thread(() -> {
            started.countDown();
            try {
                injector.inject(Category.FORCE, PATH);
            } catch (Throwable t) {
                error.set(t);
            }
            interruptedAfter.set(Thread.currentThread().isInterrupted());
        });
        worker.start();
        started.await();
        Thread.sleep(50);
        worker.interrupt();
        worker.join(1_000);
        assertFalse(worker.isAlive());
        assertNull(error.get());
        assertTrue(interruptedAfter.get());
    }

    @Test
    public void testStartedInjectorRefreshesUntilClosed() throws Exception {
        FileFaultInjector scheduled = new FileFaultInjector(config, 50);
        try {
            scheduled.start();
            scheduled.start();
            config.eio = true;
            assertTrue("refresh thread picks up the change", awaitEio(scheduled, true));

            // After close the last snapshot stays in effect: eio=false is never picked up.
            scheduled.close();
            config.eio = false;
            Thread.sleep(200);
            assertTrue("no refresh after close", awaitEio(scheduled, false));
            scheduled.start();
            Thread.sleep(200);
            assertTrue("start after close is a no-op", awaitEio(scheduled, false));
        } finally {
            scheduled.close();
        }
    }

    @Test
    public void testConfigChangeLoggedOnce() {
        assertEquals(1, logs.withTag("debug file fault injection created").size());
        injector.refresh();
        assertTrue(logs.withTag("fault config changed").isEmpty());

        config.delayMilli = 1;
        config.pathContains = "/replA/";
        for (int i = 0; i < 5; i++) {
            injector.refresh();
        }
        assertEquals(1, logs.withTag("fault config changed").size());
        String message = LogCapture.message(logs.withTag("fault config changed").get(0));
        assertTrue(message, message.contains("delayMilli=0") && message.contains("delayMilli=1"));
        assertTrue(message, message.contains("pathContains=/replA/"));
        assertEquals(Level.WARN, LogCapture.level(logs.withTag("fault config changed").get(0)));
    }

    @Test
    public void testUnreadableConfigInjectsNothing() throws Exception {
        config.eio = true;
        config.failure = new NumberFormatException("bad delay");
        for (int i = 0; i < 3; i++) {
            injector.refresh();
            injector.inject(Category.WRITE, PATH);
        }
        assertEquals(1, logs.withTag("config read failed (NumberFormatException: bad delay)").size());

        update(() -> config.failure = null);
        assertThrowsIo(Category.WRITE, PATH);
    }

    // ---- helpers ----

    private void update(Runnable change) {
        change.run();
        injector.refresh();
    }

    // An operation waiting on a 30s delay must return within about one slice after the refresh.
    private void assertReleasedWithinSlice(Runnable change) throws Exception {
        update(() -> config.delayMilli = 30_000);
        CompletableFuture<Void> waiting = CompletableFuture.runAsync(() -> {
            try {
                injector.inject(Category.WRITE, PATH);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(250);
        assertFalse(waiting.isDone());
        long changedAt = System.nanoTime();
        update(change);
        waiting.get(1, TimeUnit.SECONDS);
        assertTrue(elapsedMillis(changedAt) < 2 * FileFaultInjector.SLEEP_SLICE_MILLI);
    }

    // Polls up to 1s for an injected EIO when expected, or checks once when not.
    private static boolean awaitEio(FileFaultInjector target, boolean expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        do {
            try {
                target.inject(Category.WRITE, PATH);
            } catch (IOException e) {
                return true;
            }
            if (!expected) {
                return false;
            }
            Thread.sleep(20);
        } while (System.nanoTime() < deadline);
        return false;
    }

    private IOException assertThrowsIo(Category category, String path) {
        try {
            injector.inject(category, path);
        } catch (IOException e) {
            return e;
        }
        fail("expected injected EIO for " + category + " " + path);
        return null;
    }

    private static long elapsedMillis(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private static final class CountingConfig implements DebugFileChannelConfig {
        final DebugFileChannelConfig delegate;
        final AtomicInteger reads = new AtomicInteger();

        CountingConfig(DebugFileChannelConfig delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean isEio() {
            reads.incrementAndGet();
            return delegate.isEio();
        }

        @Override
        public long getDelayMilli() {
            reads.incrementAndGet();
            return delegate.getDelayMilli();
        }

        @Override
        public String getOps() {
            reads.incrementAndGet();
            return delegate.getOps();
        }

        @Override
        public String getPathContains() {
            reads.incrementAndGet();
            return delegate.getPathContains();
        }
    }

}
