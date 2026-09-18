package com.ctrip.xpipe.redis.comparator.compare;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator.CompareOnceResult;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.LaneBytes;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.MismatchReport;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardComparatorTest extends AbstractTest {

    @Test
    public void testConsistentStreamsComparedBytesMonotonicNoMismatch() {
        FakeLane early = lane("a:1", "rid", 100L, 32);
        FakeLane late = lane("b:1", "rid", 108L, 32);
        ShardComparator cmp = comparator(4, new RecordingReporter(), early, late);
        early.write(seq(0, 20));
        late.write(seq(8, 12));
        Assert.assertTrue(cmp.alignStart());
        Assert.assertEquals(108L, cmp.getComparedEnd());

        long last = 0;
        CompareOnceResult result;
        do {
            result = cmp.compareOnce();
            if (result == CompareOnceResult.ADVANCED) {
                Assert.assertTrue(cmp.getComparedBytes() > last);
                last = cmp.getComparedBytes();
            }
        } while (result != CompareOnceResult.NO_DATA);
        Assert.assertEquals(12L, cmp.getComparedBytes());
        Assert.assertEquals(120L, cmp.getComparedEnd());
        Assert.assertEquals(0L, cmp.getMismatchCount());
        Assert.assertEquals(0L, cmp.getCompareLostCount());
    }

    @Test
    public void testMismatchLocatesFirstOffsetReportsAllLanesAndContinues() {
        FakeLane a = lane("10.0.0.1:1", 0L, 32);
        FakeLane b = lane("10.0.0.2:1", 0L, 32);
        FakeLane c = lane("10.0.0.3:1", 0L, 32);
        RecordingReporter reporter = new RecordingReporter();
        ShardComparator cmp = comparator(8, reporter, a, b, c);
        byte[] same = seq(0, 16);
        byte[] tampered = Arrays.copyOf(same, same.length);
        tampered[5] = (byte) (same[5] + 1);
        a.write(same);
        b.write(same);
        c.write(tampered);

        Assert.assertEquals(CompareOnceResult.MISMATCH, cmp.compareOnce());
        MismatchReport report = reporter.mismatches.get(0);
        Assert.assertEquals("c1", report.getCluster());
        Assert.assertEquals("s1", report.getShard());
        Assert.assertEquals(5L, report.getMasterReplOffset());
        Assert.assertEquals(3, report.getLanes().size());
        Assert.assertArrayEquals(Arrays.copyOf(same, 8), find(report, "10.0.0.1:1").getChunk());
        Assert.assertArrayEquals(Arrays.copyOf(tampered, 8), find(report, "10.0.0.3:1").getChunk());
        Assert.assertFalse(find(report, "10.0.0.1:1").isMinority());
        Assert.assertTrue(find(report, "10.0.0.3:1").isMinority());
        Assert.assertEquals(2, report.getGroupsByValue().get(same[5]).size());

        Assert.assertEquals(1L, cmp.getMismatchCount());
        Assert.assertEquals(CompareOnceResult.ADVANCED, cmp.compareOnce());
        Assert.assertEquals(16L, cmp.getComparedBytes());
        Assert.assertEquals(1L, cmp.getMismatchCount());
    }

    @Test
    public void testOverwrittenTriggersCompareLostAndRealign() {
        FakeLane a = lane("a:1", 0L, 8);
        FakeLane b = lane("b:1", 0L, 8);
        RecordingReporter reporter = new RecordingReporter();
        ShardComparator cmp = comparator(4, reporter, a, b);
        a.write(seq(0, 4));
        b.write(seq(0, 4));
        Assert.assertEquals(CompareOnceResult.ADVANCED, cmp.compareOnce());
        a.write(seq(4, 8));
        b.write(seq(4, 8));
        Assert.assertEquals(CompareOnceResult.ADVANCED, cmp.compareOnce());
        a.write(seq(12, 8));
        b.write(seq(12, 8));
        Assert.assertEquals(12L, a.getBuffer().getBufferStart());

        Assert.assertEquals(CompareOnceResult.REALIGNED, cmp.compareOnce());
        long sPrime = Math.max(8L, Math.max(a.getBuffer().getBufferStart(), b.getBuffer().getBufferStart()));
        Assert.assertEquals(sPrime, cmp.getComparedEnd());
        Assert.assertEquals(12L, cmp.getComparedEnd());
        Assert.assertEquals(1L, cmp.getCompareLostCount());
        Assert.assertEquals(1L, cmp.getRealignCount());
        Assert.assertEquals(8L, (long) reporter.lostFrom.get(0));
        Assert.assertEquals("c1", reporter.lostCluster.get(0));
        Assert.assertEquals("s1", reporter.lostShard.get(0));
        Assert.assertEquals(0L, cmp.getMismatchCount());
        Assert.assertEquals(CompareOnceResult.ADVANCED, cmp.compareOnce());
        Assert.assertEquals(12L, cmp.getComparedBytes());
    }

    @Test
    public void testLaggingLaneComparedBytesStillGrows() {
        FakeLane slow = lane("slow:1", 0L, 16);
        FakeLane fast = lane("fast:1", 0L, 16);
        ShardComparator cmp = comparator(4, new RecordingReporter(), slow, fast);
        int slowPos = 0;
        int fastPos = 0;
        long last = 0;
        for (int round = 0; round < 2; round++) {
            while (slowPos < cmp.getComparedEnd() + 8) {
                slow.write(seq(slowPos, 8));
                slowPos += 8;
            }
            while (fastPos < slowPos) {
                fast.write(seq(fastPos, 8));
                fastPos += 8;
            }
            drain(cmp);
            Assert.assertTrue("round=" + round + " comparedBytes=" + cmp.getComparedBytes(),
                    cmp.getComparedBytes() > last);
            last = cmp.getComparedBytes();
            slow.write(seq(slowPos, 4));
            slowPos += 4;
            fast.write(seq(fastPos, 24));
            fastPos += 24;
            drain(cmp);
            while (slowPos < cmp.getComparedEnd()) {
                slow.write(seq(slowPos, 8));
                slowPos += 8;
            }
        }
        Assert.assertTrue(cmp.getCompareLostCount() > 0);
        Assert.assertEquals(0L, cmp.getMismatchCount());
    }

    @Test
    public void testAlignStartNullReplIdIsNotReady() {
        FakeLane a = lane("a:1", null, 10L, 16);
        FakeLane b = lane("b:1", "rid", 10L, 16);
        RecordingReporter reporter = new RecordingReporter();
        ShardComparator cmp = comparator(4, reporter, a, b);
        Assert.assertFalse(cmp.alignStart());
        Assert.assertFalse(cmp.isCompareOffsetAligned());
        Assert.assertEquals(0L, cmp.getReplIdMismatchCount());
        Assert.assertEquals(0, a.disconnectCount);
        Assert.assertEquals(0, b.disconnectCount);
        Assert.assertTrue(reporter.replIdMismatches.isEmpty());
    }

    @Test
    public void testAlignStartSnapshotsLaterNullAsAlignedNotMismatch() {
        FakeLane stable = lane("a:1", "rid", 10L, 16);
        FakeLane dropping = lane("b:1", "rid", 20L, 16);
        dropping.nullReplIdAfterFirstRead();
        RecordingReporter reporter = new RecordingReporter();
        ShardComparator cmp = comparator(4, reporter, stable, dropping);
        Assert.assertTrue(cmp.alignStart());
        Assert.assertEquals(20L, cmp.getComparedEnd());
        Assert.assertEquals(0L, cmp.getReplIdMismatchCount());
        Assert.assertEquals(0, stable.disconnectCount);
        Assert.assertEquals(0, dropping.disconnectCount);
        Assert.assertTrue(reporter.replIdMismatches.isEmpty());
    }

    @Test
    public void testAlignStartSnapshotsFirstLaneLaterNullDoesNotNpe() {
        FakeLane dropping = lane("a:1", "rid", 10L, 16);
        dropping.nullReplIdAfterFirstRead();
        FakeLane stable = lane("b:1", "rid", 20L, 16);
        ShardComparator cmp = comparator(4, new RecordingReporter(), dropping, stable);
        Assert.assertTrue(cmp.alignStart());
        Assert.assertEquals(20L, cmp.getComparedEnd());
        Assert.assertEquals(0L, cmp.getReplIdMismatchCount());
        Assert.assertEquals(0, dropping.disconnectCount);
        Assert.assertEquals(0, stable.disconnectCount);
    }

    @Test
    public void testReplIdMismatchResetsAllLanesWithoutCountingMismatch() {
        FakeLane a = lane("a:1", "rid-a", 10L, 16);
        FakeLane b = lane("b:1", "rid-b", 10L, 16);
        a.write(seq(0, 8));
        b.write(seq(0, 8));
        StreamRingBuffer oldA = a.getBuffer();
        StreamRingBuffer oldB = b.getBuffer();
        RecordingReporter reporter = new RecordingReporter();
        ShardComparator cmp = comparator(4, reporter, a, b);
        Assert.assertEquals(CompareOnceResult.REPL_ID_RESET, cmp.compareOnce());
        Assert.assertEquals(1, a.disconnectCount);
        Assert.assertEquals(1, b.disconnectCount);
        Assert.assertEquals(1, a.reconnectCount);
        Assert.assertEquals(1, b.reconnectCount);
        Assert.assertNotSame(oldA, a.getBuffer());
        Assert.assertNotSame(oldB, b.getBuffer());
        Assert.assertNull(a.getReplId());
        Assert.assertEquals(1L, cmp.getReplIdMismatchCount());
        Assert.assertEquals(0L, cmp.getMismatchCount());
        Assert.assertFalse(cmp.isCompareOffsetAligned());
        Assert.assertEquals(Arrays.asList("rid-a", "rid-b"), reporter.replIdMismatches.get(0));
        Assert.assertEquals("c1", reporter.replIdCluster.get(0));
        Assert.assertEquals("s1", reporter.replIdShard.get(0));
        Assert.assertEquals(CompareOnceResult.NO_DATA, cmp.compareOnce());
        Assert.assertEquals(1, a.disconnectCount);
    }

    @Test
    public void testReplaceLanesUsesNewArrayAndResetsCompareOffsetAligned() {
        FakeLane a = lane("a:1", 0L, 32);
        FakeLane b = lane("b:1", 0L, 32);
        FakeLane c = lane("c:1", 20L, 32);
        a.write(seq(0, 8));
        b.write(seq(0, 8));
        ShardComparator cmp = comparator(4, new RecordingReporter(), a, b);
        Assert.assertEquals(CompareOnceResult.ADVANCED, cmp.compareOnce());
        Assert.assertTrue(cmp.isCompareOffsetAligned());
        Assert.assertEquals(4L, cmp.getComparedEnd());

        c.write(seq(0, 8));
        b.write(seq(8, 8));
        cmp.replaceLanes(Arrays.asList(b, c));
        Assert.assertFalse(cmp.isCompareOffsetAligned());
        CompareLane[] bound = cmp.getLanes();
        Assert.assertEquals(2, bound.length);
        Assert.assertSame(b, bound[0]);
        Assert.assertSame(c, bound[1]);
        Assert.assertEquals(0, a.disconnectCount);

        Assert.assertTrue(cmp.alignStart());
        Assert.assertEquals(20L, cmp.getComparedEnd());
        Assert.assertEquals(0, a.disconnectCount);
    }

    @Test
    public void testStopDoesNotCloseLanesAndRejectsReuse() {
        FakeLane a = lane("a:1", 0L, 32);
        FakeLane b = lane("b:1", 0L, 32);
        ShardComparator cmp = comparator(4, new RecordingReporter(), a, b);
        cmp.stop();
        Assert.assertEquals(0, a.disconnectCount);
        Assert.assertEquals(0, b.disconnectCount);
        Assert.assertEquals(0, cmp.getLanes().length);
        Assert.assertFalse(cmp.isCompareOffsetAligned());
        try {
            cmp.replaceLanes(Arrays.asList(a, b));
            Assert.fail("replace after stop");
        } catch (IllegalStateException expected) {
        }
        try {
            cmp.start();
            Assert.fail("start after stop");
        } catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testNoSkipOrThread() throws Exception {
        String text = new String(Files.readAllBytes(
                Paths.get("src/main/java/com/ctrip/xpipe/redis/comparator/compare/ShardComparator.java")),
                StandardCharsets.UTF_8);
        Assert.assertFalse(text.contains("void skip(") || text.contains("skipUntil("));
        Assert.assertFalse(text.contains("clearBuffer"));
        Assert.assertFalse(text.contains("new Thread(") || text.contains("MetricProxy"));
        Assert.assertFalse(text.contains(".join("));
        Assert.assertFalse(text.contains("void signal(") || text.contains(".signal("));
        Assert.assertFalse(text.contains("releaseLanes"));
        Assert.assertTrue(text.contains("XpipeThreadFactory"));
        Assert.assertTrue(text.contains("void wake("));
        String stopBody = methodBody(text, "public void stop()");
        Assert.assertFalse(stopBody.contains("notify"));
        Assert.assertFalse(stopBody.contains("wake("));
        Assert.assertFalse(stopBody.contains("signal"));
        Assert.assertFalse(stopBody.contains(".join("));
        Assert.assertFalse(stopBody.contains("interrupt"));
    }

    @Test
    public void testStartFailureRollsBackAndAllowsRetry() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        ThreadFactory factory = r -> {
            if (attempts.getAndIncrement() == 0) {
                throw new RuntimeException("oom-sim");
            }
            return XpipeThreadFactory.create("shard-compare-retry", true).newThread(r);
        };
        FakeLane a = lane("a:1", 0L, 32);
        FakeLane b = lane("b:1", 0L, 32);
        ShardComparator cmp = new ShardComparator("c1", "s1", Arrays.asList(a, b),
                new ComparatorConfig() {
                    @Override
                    public int getCompareChunkBytes() {
                        return 4;
                    }
                }, new RecordingReporter(), factory);
        try {
            cmp.start();
            Assert.fail("first start should fail");
        } catch (RuntimeException e) {
            Assert.assertEquals("oom-sim", e.getMessage());
        }
        Assert.assertFalse(cmp.isRunning());
        Assert.assertNull(cmp.getCompareThread());
        cmp.start();
        Assert.assertTrue(cmp.isRunning());
        waitConditionUntilTimeOut(() -> cmp.getCompareThread() != null && cmp.getCompareThread().isAlive());
        Thread t = cmp.getCompareThread();
        cmp.stop();
        waitConditionUntilTimeOut(() -> !t.isAlive(), ComparatorConstants.COMPARE_STOP_JOIN_MILLI);
    }

    @Test
    public void testReplIdMismatchReporterRunsWithoutComparatorLock() {
        FakeLane a = lane("a:1", "rid-a", 10L, 16);
        FakeLane b = lane("b:1", "rid-b", 10L, 16);
        AtomicBoolean held = new AtomicBoolean(true);
        ShardComparator[] box = new ShardComparator[1];
        CompareReporter reporter = new CompareReporter() {
            @Override
            public void onMismatch(MismatchReport report) {
            }

            @Override
            public void onCompareLost(String cluster, String shard, long from, long to) {
            }

            @Override
            public void onReplIdMismatch(String cluster, String shard, List<String> replIds) {
                held.set(Thread.holdsLock(box[0]));
            }
        };
        box[0] = comparator(4, reporter, a, b);
        Assert.assertEquals(CompareOnceResult.REPL_ID_RESET, box[0].compareOnce());
        Assert.assertFalse(held.get());
        Assert.assertEquals(1, a.disconnectCount);
        Assert.assertEquals(1, b.disconnectCount);
    }

    @Test
    public void testReplIdMismatchSkipsLanesReplacedBeforeReset() {
        FakeLane a = lane("a:1", "rid-a", 10L, 16);
        FakeLane b = lane("b:1", "rid-b", 10L, 16);
        FakeLane c = lane("c:1", "rid-c", 20L, 16);
        ShardComparator[] box = new ShardComparator[1];
        CompareReporter reporter = new CompareReporter() {
            @Override
            public void onMismatch(MismatchReport report) {
            }

            @Override
            public void onCompareLost(String cluster, String shard, long from, long to) {
            }

            @Override
            public void onReplIdMismatch(String cluster, String shard, List<String> replIds) {
                box[0].replaceLanes(Arrays.asList(b, c));
            }
        };
        box[0] = comparator(4, reporter, a, b);
        Assert.assertEquals(CompareOnceResult.REPL_ID_RESET, box[0].compareOnce());
        Assert.assertEquals(0, a.disconnectCount);
        Assert.assertEquals(1, b.disconnectCount);
        Assert.assertEquals(1, b.reconnectCount);
        Assert.assertEquals(0, c.disconnectCount);
        CompareLane[] bound = box[0].getLanes();
        Assert.assertSame(b, bound[0]);
        Assert.assertSame(c, bound[1]);
    }

    @Test
    public void testConstructorAllowsZeroLanesAndWakeAfterStopDoesNotThrow() {
        ShardComparator cmp = new ShardComparator("c1", "s1", Collections.emptyList(),
                new ComparatorConfig() {
                    @Override
                    public int getCompareChunkBytes() {
                        return 4;
                    }
                }, new RecordingReporter());
        Assert.assertEquals(0, cmp.getLanes().length);
        Assert.assertEquals(CompareOnceResult.NO_DATA, cmp.compareOnce());
        cmp.stop();
        cmp.wake();
        Assert.assertFalse(cmp.isRunning());
    }

    private static ShardComparator comparator(int chunk, CompareReporter reporter, FakeLane... lanes) {
        return new ShardComparator("c1", "s1", Arrays.asList(lanes), new ComparatorConfig() {
            @Override
            public int getCompareChunkBytes() {
                return chunk;
            }
        }, reporter);
    }

    private static FakeLane lane(String address, long continueOffset, int capacity) {
        return lane(address, "rid", continueOffset, capacity);
    }

    private static FakeLane lane(String address, String replId, long continueOffset, int capacity) {
        return new FakeLane(address, replId, continueOffset, capacity);
    }

    private static void drain(ShardComparator cmp) {
        CompareOnceResult result;
        int steps = 0;
        do {
            result = cmp.compareOnce();
            Assert.assertTrue(++steps < 1000);
        } while (result != CompareOnceResult.NO_DATA && result != CompareOnceResult.REPL_ID_RESET);
    }

    private static LaneBytes find(MismatchReport report, String address) {
        for (LaneBytes lane : report.getLanes()) {
            if (address.equals(lane.getAddress())) {
                return lane;
            }
        }
        throw new AssertionError("missing " + address);
    }

    private static String methodBody(String text, String signature) {
        int start = text.indexOf(signature);
        Assert.assertTrue(signature, start > 0);
        int brace = text.indexOf('{', start);
        Assert.assertTrue(signature, brace > start);
        int depth = 0;
        for (int i = brace; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return text.substring(brace, i + 1);
                }
            }
        }
        throw new AssertionError("unclosed " + signature);
    }

    private static byte[] seq(int from, int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = (byte) (from + i);
        }
        return out;
    }

    static final class FakeLane implements CompareLane {
        private final String address;
        private final int capacity;
        private String replId;
        private final long continueOffset;
        private StreamRingBuffer buffer;
        private int replIdReadsLeft = -1;
        int disconnectCount;
        int reconnectCount;

        FakeLane(String address, String replId, long continueOffset, int capacity) {
            this.address = address;
            this.replId = replId;
            this.continueOffset = continueOffset;
            this.capacity = capacity;
            this.buffer = new StreamRingBuffer(capacity, continueOffset);
        }

        void write(byte[] bytes) {
            buffer.write(Unpooled.wrappedBuffer(bytes));
        }

        void nullReplIdAfterFirstRead() {
            replIdReadsLeft = 1;
        }

        @Override
        public String getAddress() { return address; }

        @Override
        public String getReplId() {
            if (replIdReadsLeft >= 0) {
                if (replIdReadsLeft == 0) {
                    return null;
                }
                replIdReadsLeft--;
            }
            return replId;
        }

        @Override
        public long getContinueOffset() { return continueOffset; }

        @Override
        public StreamRingBuffer getBuffer() { return buffer; }

        @Override
        public void disconnect() {
            disconnectCount++;
            replId = null;
        }

        @Override
        public void reconnect() {
            reconnectCount++;
            replId = null;
            buffer = new StreamRingBuffer(capacity, continueOffset);
        }

        @Override
        public void close() {
            disconnect();
        }
    }

    static final class RecordingReporter implements CompareReporter {
        final List<MismatchReport> mismatches = new ArrayList<>();
        final List<Long> lostFrom = new ArrayList<>();
        final List<String> lostCluster = new ArrayList<>();
        final List<String> lostShard = new ArrayList<>();
        final List<List<String>> replIdMismatches = new ArrayList<>();
        final List<String> replIdCluster = new ArrayList<>();
        final List<String> replIdShard = new ArrayList<>();

        @Override
        public void onMismatch(MismatchReport report) { mismatches.add(report); }

        @Override
        public void onCompareLost(String cluster, String shard, long from, long to) {
            lostFrom.add(from);
            lostCluster.add(cluster);
            lostShard.add(shard);
        }

        @Override
        public void onReplIdMismatch(String cluster, String shard, List<String> replIds) {
            replIdMismatches.add(replIds);
            replIdCluster.add(cluster);
            replIdShard.add(shard);
        }
    }
}
