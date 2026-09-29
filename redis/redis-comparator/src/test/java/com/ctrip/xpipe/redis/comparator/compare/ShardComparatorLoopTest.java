package com.ctrip.xpipe.redis.comparator.compare;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparatorTest.FakeLane;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparatorTest.RecordingReporter;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * T-CX.4 ①③：专属比对线程名、有界等待与超时自旋（AC-14b ②③）。
 */
public class ShardComparatorLoopTest extends AbstractTest {

    @Test
    public void testThreadNameContainsClusterShardAndIsDaemon() throws Exception {
        FakeLane a = lane("a:1");
        FakeLane b = lane("b:1");
        ShardComparator cmp = comparator(new RecordingReporter(), a, b);
        Thread t;
        try {
            cmp.start();
            waitConditionUntilTimeOut(() -> cmp.getCompareThread() != null && cmp.getCompareThread().isAlive());
            t = cmp.getCompareThread();
            Assert.assertTrue(t.getName().contains("c1"));
            Assert.assertTrue(t.getName().contains("s1"));
            Assert.assertTrue(t.getName().contains(ComparatorConstants.COMPARE_THREAD_NAME_PREFIX));
            Assert.assertTrue(t.isDaemon());
            Assert.assertTrue(cmp.isRunning());
        } finally {
            cmp.stop();
        }
        waitConditionUntilTimeOut(() -> !cmp.isRunning() && !t.isAlive());
    }

    @Test
    public void testStopDoesNotJoinCompareThread() throws Exception {
        FakeLane a = lane("a:1");
        FakeLane b = lane("b:1");
        ShardComparator cmp = comparator(new RecordingReporter(), a, b);
        cmp.start();
        waitConditionUntilTimeOut(() -> cmp.getCompareThread() != null && cmp.getCompareThread().isAlive());
        Thread t = cmp.getCompareThread();
        long begin = System.nanoTime();
        cmp.stop();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
        Assert.assertFalse(cmp.isRunning());
        Assert.assertTrue("stop must not join, took " + elapsed + "ms",
                elapsed < ComparatorConstants.COMPARE_WAIT_MILLI);
        Assert.assertTrue("stop must not wake the waiting thread immediately", t.isAlive());
        waitConditionUntilTimeOut(() -> !t.isAlive(), ComparatorConstants.COMPARE_STOP_JOIN_MILLI);
    }

    @Test
    public void testOnCommandsWakeWakesCompareThreadUnderWaitTimeout() throws Exception {
        FakeLane a = lane("a:1");
        FakeLane b = lane("b:1");
        ShardComparator cmp = comparator(new RecordingReporter(), a, b);
        try {
            cmp.start();
            waitConditionUntilTimeOut(() -> cmp.getCompareThread() != null
                    && cmp.getCompareThread().getState() == Thread.State.TIMED_WAITING);
            byte[] payload = seq(0, 8);
            a.write(payload);
            b.write(payload);
            long begin = System.nanoTime();
            cmp.wake();
            waitConditionUntilTimeOut(() -> cmp.getComparedBytes() >= 8, 50);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
            Assert.assertEquals(8L, cmp.getComparedBytes());
            Assert.assertTrue("onCommands must wake under COMPARE_WAIT_MILLI, took " + elapsed + "ms",
                    elapsed < ComparatorConstants.COMPARE_WAIT_MILLI);
        } finally {
            cmp.stop();
        }
    }

    @Test
    public void testStoppedInstanceWakeDoesNotWakeNewComparator() throws Exception {
        FakeLane oldA = lane("a:1");
        FakeLane oldB = lane("b:1");
        ShardComparator old = comparator(new RecordingReporter(), oldA, oldB);
        old.start();
        waitConditionUntilTimeOut(() -> old.getCompareThread() != null && old.getCompareThread().isAlive());
        Thread oldThread = old.getCompareThread();
        old.stop();
        waitConditionUntilTimeOut(() -> !oldThread.isAlive(), ComparatorConstants.COMPARE_STOP_JOIN_MILLI);
        old.wake();

        FakeLane a = lane("a:1");
        FakeLane b = lane("b:1");
        ShardComparator neu = comparator(new RecordingReporter(), a, b);
        try {
            neu.start();
            waitConditionUntilTimeOut(() -> neu.getCompareThread() != null
                    && neu.getCompareThread().getState() == Thread.State.TIMED_WAITING);
            neu.wake();
            waitConditionUntilTimeOut(() -> neu.getCompareThread().getState() == Thread.State.TIMED_WAITING);
            byte[] payload = seq(0, 8);
            a.write(payload);
            b.write(payload);
            old.wake();
            try {
                waitConditionUntilTimeOut(() -> neu.getComparedBytes() > 0,
                        ComparatorConstants.COMPARE_WAIT_MILLI / 5);
                Assert.fail("stopped instance wake must not progress a new comparator");
            } catch (TimeoutException expected) {
                Assert.assertEquals(0L, neu.getComparedBytes());
            }
            long begin = System.nanoTime();
            neu.wake();
            waitConditionUntilTimeOut(() -> neu.getComparedBytes() >= 8, 50);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
            Assert.assertEquals(8L, neu.getComparedBytes());
            Assert.assertTrue("new instance must be woken by its own wake, took " + elapsed + "ms",
                    elapsed < ComparatorConstants.COMPARE_WAIT_MILLI);
        } finally {
            neu.stop();
        }
    }

    @Test
    public void testWaitTimeoutSpinsWithoutSignal() throws Exception {
        FakeLane a = lane("a:1");
        FakeLane b = lane("b:1");
        RecordingReporter reporter = new RecordingReporter();
        ShardComparator cmp = comparator(reporter, a, b);
        try {
            cmp.start();
            waitConditionUntilTimeOut(() -> cmp.getCompareThread() != null
                    && cmp.getCompareThread().getState() == Thread.State.TIMED_WAITING);
            Thread compareThread = cmp.getCompareThread();
            Assert.assertEquals(Thread.State.TIMED_WAITING, compareThread.getState());

            byte[] first = seq(0, 8);
            a.write(first);
            b.write(first);
            long begin = System.nanoTime();
            waitConditionUntilTimeOut(() -> cmp.getComparedBytes() >= 8,
                    ComparatorConstants.COMPARE_WAIT_MILLI + 400);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
            Assert.assertEquals(8L, cmp.getComparedBytes());
            Assert.assertTrue("must spin within wait+slack, took " + elapsed + "ms",
                    elapsed <= ComparatorConstants.COMPARE_WAIT_MILLI + 400);

            waitConditionUntilTimeOut(() -> compareThread.getState() == Thread.State.TIMED_WAITING);
            byte[] second = seq(8, 8);
            a.write(second);
            b.write(second);
            waitConditionUntilTimeOut(() -> cmp.getComparedBytes() >= 16,
                    ComparatorConstants.COMPARE_WAIT_MILLI + 400);
            Assert.assertEquals(16L, cmp.getComparedBytes());
        } finally {
            cmp.stop();
        }
    }

    @Test
    public void testMismatchReportedOnCompareThread() throws Exception {
        FakeLane a = lane("a:1");
        FakeLane b = lane("b:1");
        AtomicReference<Thread> reportThread = new AtomicReference<>();
        ShardComparator cmp = comparator(new CompareReporter() {
            @Override
            public void onMismatch(MismatchReport report) {
                reportThread.set(Thread.currentThread());
            }

            @Override
            public void onCompareLost(String cluster, String shard, long from, long to) {
            }

            @Override
            public void onReplIdMismatch(String cluster, String shard, java.util.List<String> replIds) {
            }
        }, a, b);
        try {
            cmp.start();
            waitConditionUntilTimeOut(() -> cmp.getCompareThread() != null
                    && cmp.getCompareThread().getState() == Thread.State.TIMED_WAITING);
            byte[] left = seq(0, 8);
            byte[] right = Arrays.copyOf(left, left.length);
            right[0] = (byte) (left[0] + 1);
            a.write(left);
            b.write(right);
            waitConditionUntilTimeOut(() -> reportThread.get() != null,
                    ComparatorConstants.COMPARE_WAIT_MILLI + 400);
            Assert.assertSame(cmp.getCompareThread(), reportThread.get());
            Assert.assertEquals(1L, cmp.getMismatchCount());
        } finally {
            cmp.stop();
        }
    }

    @Test
    public void testZeroLaneStartThenReplaceLanesAndWake() throws Exception {
        FakeLane a = lane("a:1");
        FakeLane b = lane("b:1");
        ShardComparator cmp = new ShardComparator("c1", "s1", Collections.emptyList(),
                new ComparatorConfig() {
                    @Override
                    public int getCompareChunkBytes() {
                        return 8;
                    }
                }, new RecordingReporter());
        try {
            cmp.start();
            waitConditionUntilTimeOut(() -> cmp.getCompareThread() != null
                    && cmp.getCompareThread().getState() == Thread.State.TIMED_WAITING);
            cmp.replaceLanes(Arrays.asList(a, b));
            byte[] payload = seq(0, 8);
            a.write(payload);
            b.write(payload);
            long begin = System.nanoTime();
            cmp.wake();
            waitConditionUntilTimeOut(() -> cmp.getComparedBytes() >= 8, 50);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
            Assert.assertEquals(8L, cmp.getComparedBytes());
            Assert.assertTrue("wake after replaceLanes must be under COMPARE_WAIT_MILLI, took " + elapsed + "ms",
                    elapsed < ComparatorConstants.COMPARE_WAIT_MILLI);
        } finally {
            cmp.stop();
        }
    }

    private static ShardComparator comparator(CompareReporter reporter, FakeLane... lanes) {
        return new ShardComparator("c1", "s1", Arrays.asList(lanes), new ComparatorConfig() {
            @Override
            public int getCompareChunkBytes() {
                return 8;
            }
        }, reporter);
    }

    private static FakeLane lane(String address) {
        return new FakeLane(address, "rid", 0L, 32);
    }

    private static byte[] seq(int from, int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = (byte) (from + i);
        }
        return out;
    }
}
