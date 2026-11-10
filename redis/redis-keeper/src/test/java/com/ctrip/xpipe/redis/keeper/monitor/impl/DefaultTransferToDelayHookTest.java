package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.storage.LogCapture;
import com.ctrip.xpipe.redis.keeper.storage.TransferToDelayContext;
import com.ctrip.xpipe.redis.keeper.storage.TransferToDelayHook;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTransferToDelayHookTest {

    private static final String LOG_TAG = "[transferToDelay]";

    private static final long POSITION = 100L;

    private static final long OFFSET = 4196L;

    private RedisSlave redisSlave;

    private AtomicLong ack;

    @Before
    public void setUp() {
        ack = new AtomicLong(10L);
        redisSlave = mock(RedisSlave.class);
        when(redisSlave.toString()).thenAnswer(invocation -> "test-slave[ack:" + ack.get() + "]");
    }

    @Test
    public void testDisabledAndNormalDelayLogNothing() {
        try (LogCapture logs = new LogCapture(DefaultTransferToDelayHook.class)) {
            DefaultTransferToDelayHook disabled = hook(() -> 0, 2);
            assertSame(TransferToDelayContext.NONE, disabled.begin(POSITION, OFFSET));

            DefaultTransferToDelayHook negative = hook(() -> -1, 2);
            assertSame(TransferToDelayContext.NONE, negative.begin(POSITION, OFFSET));

            DefaultTransferToDelayHook enabled = hook(() -> Integer.MAX_VALUE, 2);
            assertSame(TransferToDelayContext.NONE, enabled.begin(POSITION, Long.MIN_VALUE));
            TransferToDelayContext context = enabled.begin(POSITION, OFFSET);
            for (TransferToDelayContext.Stage stage : TransferToDelayContext.Stage.values()) {
                context.begin(stage);
                context.end(stage);
            }
            context.complete(OFFSET - POSITION, null);

            assertTrue(logs.withTag(LOG_TAG).isEmpty());
        }
    }

    @Test
    public void testEachSlowStageLogsCurrentSlaveAndTransferInfo() {
        try (LogCapture logs = new LogCapture(DefaultTransferToDelayHook.class)) {
            TransferToDelayContext context = hook(() -> 100, 2).begin(POSITION, OFFSET);
            ack.set(20L);

            for (TransferToDelayContext.Stage stage : TransferToDelayContext.Stage.values()) {
                context.begin(stage);
                elapseOneMillisecond();
                context.end(stage);
            }
            context.complete(OFFSET - POSITION, null);

            List<LogEvent> events = logs.withTag("[transferToDelay][repl_42]");
            assertEquals(TransferToDelayContext.Stage.values().length, events.size());
            for (TransferToDelayContext.Stage stage : TransferToDelayContext.Stage.values()) {
                List<LogEvent> stageEvents = logs.withTag("[transferToDelay][repl_42][" + stage + "]");
                assertEquals(1, stageEvents.size());
                LogEvent event = stageEvents.get(0);
                String message = LogCapture.message(event);
                assertEquals(Level.INFO, event.getLevel());
                assertTrue(message, message.contains("test-slave[ack:20]"));
                assertTrue(message, message.contains("position:" + POSITION));
                assertTrue(message, message.contains("offset:" + OFFSET));
                assertTrue(message, message.contains("delay:") && message.contains("us"));
            }
        }
    }

    @Test
    public void testCompleteSettlesOnlyActiveStagesOnce() {
        try (LogCapture logs = new LogCapture(DefaultTransferToDelayHook.class)) {
            TransferToDelayContext context = hook(() -> 100, 2).begin(POSITION, OFFSET);
            context.begin(TransferToDelayContext.Stage.CACHE_GET);
            context.begin(TransferToDelayContext.Stage.FILE_TRANSFER);
            elapseOneMillisecond();

            context.complete(0L, new IOException("injected"));
            assertEquals(1, logs.withTag("[CACHE_GET]").size());
            assertEquals(1, logs.withTag("[FILE_TRANSFER]").size());
            assertEquals(2, logs.withTag(LOG_TAG).size());

            context.end(TransferToDelayContext.Stage.CACHE_GET);
            context.end(TransferToDelayContext.Stage.FILE_TRANSFER);
            context.complete(0L, null);
            assertEquals(2, logs.withTag(LOG_TAG).size());
        }
    }

    @Test
    public void testSlotOverwriteDropsOldContextByOffset() {
        try (LogCapture logs = new LogCapture(DefaultTransferToDelayHook.class)) {
            DefaultTransferToDelayHook hook = hook(() -> 100, 1);
            TransferToDelayContext oldContext = hook.begin(100L, 111L);
            oldContext.begin(TransferToDelayContext.Stage.FILE_TRANSFER);

            TransferToDelayContext currentContext = hook.begin(200L, 222L);
            currentContext.begin(TransferToDelayContext.Stage.FILE_TRANSFER);
            elapseOneMillisecond();

            oldContext.end(TransferToDelayContext.Stage.FILE_TRANSFER);
            oldContext.complete(0L, null);
            currentContext.end(TransferToDelayContext.Stage.FILE_TRANSFER);
            currentContext.complete(22L, null);

            List<LogEvent> events = logs.withTag(LOG_TAG);
            assertEquals(1, events.size());
            String message = LogCapture.message(events.get(0));
            assertTrue(message, message.contains("position:200"));
            assertTrue(message, message.contains("offset:222"));
            assertTrue(message, !message.contains("position:100"));
        }
    }

    @Test
    public void testThresholdFailureIsIsolatedBySafeBegin() {
        DefaultTransferToDelayHook hook = hook(() -> {
            throw new IllegalStateException("injected");
        }, 1);

        TransferToDelayContext context = TransferToDelayHook.safeBegin(hook, POSITION, OFFSET);
        assertSame(TransferToDelayContext.NONE, context);
    }

    private DefaultTransferToDelayHook hook(java.util.function.IntSupplier threshold, int slots) {
        return new DefaultTransferToDelayHook(ReplId.from(42L), redisSlave, threshold, slots);
    }

    private static void elapseOneMillisecond() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1L);
        while (System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }
}
