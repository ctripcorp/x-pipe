package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.utils.OffsetNotifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * keeper-tfs-m6 P15a (spec §15.2): {@link CoalescingOffsetNotifier} thresholds, switch, flush / close and failure handling.
 */
public class CoalescingOffsetNotifierTest {

    private static final int BYTES_THRESHOLD = 100;

    private static final int TIME_THRESHOLD = 5;

    private final AtomicInteger bytesThreshold = new AtomicInteger(BYTES_THRESHOLD);

    private final AtomicInteger timeThreshold = new AtomicInteger(TIME_THRESHOLD);

    private final AtomicBoolean enabled = new AtomicBoolean(true);

    private RecordingNotifier notifier;

    private ManualScheduler scheduler;

    private ScheduledExecutorService realScheduler;

    @Before
    public void setUp() {
        notifier = new RecordingNotifier();
        scheduler = new ManualScheduler();
    }

    @After
    public void tearDown() {
        if (realScheduler != null) {
            realScheduler.shutdownNow();
        }
    }

    @Test
    public void testBytesThresholdNotifiesImmediatelyAndCancelsTimer() {
        CoalescingOffsetNotifier coalescer = create(scheduler.executor);

        coalescer.onWritten(40, 39);
        assertEquals(1, scheduler.tasks.size());
        assertTrue(notifier.offsets.isEmpty());

        coalescer.onWritten(60, 99);
        assertEquals(Collections.singletonList(99L), notifier.offsets);
        verify(scheduler.futures.get(0)).cancel(false);

        // a late callback of the cancelled timer has nothing to notify
        scheduler.runNext();
        assertEquals(Collections.singletonList(99L), notifier.offsets);
    }

    @Test
    public void testBelowThresholdNotifiesAfterTimeThresholdAndSchedulesOnce() {
        CoalescingOffsetNotifier coalescer = create(scheduler.executor);

        coalescer.onWritten(10, 9);
        coalescer.onWritten(10, 19);
        coalescer.onWritten(10, 29);
        assertEquals(1, scheduler.tasks.size());
        assertEquals(Collections.singletonList((long) TIME_THRESHOLD), scheduler.delays);
        assertTrue(notifier.offsets.isEmpty());

        scheduler.runNext();
        assertEquals(Collections.singletonList(29L), notifier.offsets);

        // timer slot is released: the next write schedules again
        coalescer.onWritten(10, 39);
        assertEquals(1, scheduler.tasks.size());
        scheduler.runNext();
        assertEquals(Arrays.asList(29L, 39L), notifier.offsets);
    }

    @Test
    public void testTimerWakesWaitingReaderOnRealScheduler() throws Exception {
        realScheduler = Executors.newSingleThreadScheduledExecutor();
        timeThreshold.set(50);
        OffsetNotifier offsetNotifier = new OffsetNotifier(-1);
        CoalescingOffsetNotifier coalescer = new CoalescingOffsetNotifier(offsetNotifier, bytesThreshold::get,
                timeThreshold::get, enabled::get, realScheduler);

        long begin = System.nanoTime();
        coalescer.onWritten(10, 9);
        assertFalse(offsetNotifier.await(9, 1));
        assertTrue(offsetNotifier.await(9, 2000));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) >= 40);
    }

    @Test
    public void testDisabledNotifiesEveryWrite() {
        enabled.set(false);
        CoalescingOffsetNotifier coalescer = create(scheduler.executor);

        coalescer.onWritten(10, 9);
        coalescer.onWritten(10, 19);
        assertEquals(Arrays.asList(9L, 19L), notifier.offsets);
        verify(scheduler.executor, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testNullSchedulerNotifiesEveryWrite() {
        CoalescingOffsetNotifier coalescer = create(null);

        coalescer.onWritten(10, 9);
        coalescer.onWritten(10, 19);
        assertEquals(Arrays.asList(9L, 19L), notifier.offsets);
    }

    @Test
    public void testIgnoresNonPositiveWrite() {
        CoalescingOffsetNotifier coalescer = create(null);

        coalescer.onWritten(0, 9);
        coalescer.onWritten(-1, 19);
        assertTrue(notifier.offsets.isEmpty());
    }

    @Test
    public void testRuntimeSwitchAndThresholdChangeTakeEffectOnNextCall() {
        CoalescingOffsetNotifier coalescer = create(scheduler.executor);

        // pending under coalescing, then switch off: next write flushes everything and cancels the timer
        coalescer.onWritten(10, 9);
        assertEquals(1, scheduler.tasks.size());
        enabled.set(false);
        coalescer.onWritten(10, 19);
        assertEquals(Collections.singletonList(19L), notifier.offsets);
        verify(scheduler.futures.get(0)).cancel(false);
        scheduler.runNext();
        assertEquals(Collections.singletonList(19L), notifier.offsets);

        // switch on again with a new time threshold
        enabled.set(true);
        timeThreshold.set(7);
        coalescer.onWritten(10, 29);
        assertEquals(Long.valueOf(7), scheduler.delays.get(scheduler.delays.size() - 1));
        assertEquals(Collections.singletonList(19L), notifier.offsets);

        // lowering bytes threshold makes the next write notify at once
        bytesThreshold.set(15);
        coalescer.onWritten(10, 39);
        assertEquals(Arrays.asList(19L, 39L), notifier.offsets);

        // non-positive thresholds are clamped to 1
        bytesThreshold.set(BYTES_THRESHOLD);
        timeThreshold.set(0);
        coalescer.onWritten(10, 49);
        assertEquals(Long.valueOf(1), scheduler.delays.get(scheduler.delays.size() - 1));
        bytesThreshold.set(0);
        coalescer.onWritten(1, 50);
        assertEquals(Arrays.asList(19L, 39L, 50L), notifier.offsets);
    }

    @Test
    public void testFlushNotifiesPendingAndCancelsTimer() {
        CoalescingOffsetNotifier coalescer = create(scheduler.executor);

        coalescer.flush();
        assertTrue(notifier.offsets.isEmpty());

        coalescer.onWritten(10, 9);
        coalescer.flush();
        assertEquals(Collections.singletonList(9L), notifier.offsets);
        verify(scheduler.futures.get(0)).cancel(false);

        coalescer.flush();
        assertEquals(Collections.singletonList(9L), notifier.offsets);
    }

    @Test
    public void testCloseNotifiesPendingThenIgnoresEverything() {
        CoalescingOffsetNotifier coalescer = create(scheduler.executor);

        coalescer.onWritten(10, 9);
        coalescer.close();
        assertEquals(Collections.singletonList(9L), notifier.offsets);
        verify(scheduler.futures.get(0)).cancel(false);

        coalescer.onWritten(200, 209);
        coalescer.flush();
        coalescer.close();
        // late callback of the cancelled timer
        scheduler.runNext();
        assertEquals(Collections.singletonList(9L), notifier.offsets);
        assertTrue(scheduler.tasks.isEmpty());
    }

    @Test
    public void testNotifyFailureDoesNotPropagate() {
        enabled.set(false);
        CoalescingOffsetNotifier coalescer = create(scheduler.executor);

        notifier.failNext = true;
        coalescer.onWritten(10, 9);
        assertEquals(Collections.singletonList(9L), notifier.offsets);

        coalescer.onWritten(10, 19);
        assertEquals(Arrays.asList(9L, 19L), notifier.offsets);

        enabled.set(true);
        coalescer.onWritten(10, 29);
        notifier.failNext = true;
        scheduler.runNext();
        assertEquals(Arrays.asList(9L, 19L, 29L), notifier.offsets);

        // timer slot released after a failed timer notify
        coalescer.onWritten(10, 39);
        assertEquals(1, scheduler.tasks.size());
    }

    @Test
    public void testScheduleRejectedNotifiesNow() {
        ScheduledExecutorService rejecting = mock(ScheduledExecutorService.class);
        when(rejecting.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenThrow(new RejectedExecutionException("event loop shutdown"));
        CoalescingOffsetNotifier coalescer = create(rejecting);

        coalescer.onWritten(10, 9);
        assertEquals(Collections.singletonList(9L), notifier.offsets);

        coalescer.onWritten(10, 19);
        assertEquals(Arrays.asList(9L, 19L), notifier.offsets);
    }

    private CoalescingOffsetNotifier create(ScheduledExecutorService executor) {
        return new CoalescingOffsetNotifier(notifier, bytesThreshold::get, timeThreshold::get, enabled::get, executor);
    }

    private static final class RecordingNotifier extends OffsetNotifier {

        private final List<Long> offsets = new ArrayList<>();

        private boolean failNext;

        RecordingNotifier() {
            super(-1);
        }

        @Override
        public void offsetIncreased(long newOffset) {
            offsets.add(newOffset);
            if (failNext) {
                failNext = false;
                throw new IllegalStateException("inject notify fail");
            }
            super.offsetIncreased(newOffset);
        }
    }

    private static final class ManualScheduler {

        private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

        private final List<Runnable> tasks = new ArrayList<>();

        private final List<Long> delays = new ArrayList<>();

        private final List<ScheduledFuture<?>> futures = new ArrayList<>();

        ManualScheduler() {
            when(executor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(inv -> {
                tasks.add(inv.getArgument(0));
                delays.add(((TimeUnit) inv.getArgument(2)).toMillis(inv.getArgument(1)));
                ScheduledFuture<?> future = mock(ScheduledFuture.class);
                futures.add(future);
                return future;
            });
        }

        void runNext() {
            assertFalse("no scheduled task", tasks.isEmpty());
            tasks.remove(0).run();
        }
    }
}
