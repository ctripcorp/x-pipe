package com.ctrip.xpipe.redis.comparator.report;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator.CompareOnceResult;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager.ShardCompareTask;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultCompareReporterTest extends AbstractTest {

    @Test
    public void testFormatContainsOffsetLanesHexAndGroups() {
        DefaultCompareReporter reporter = reporter(new RecordingMonitor(), new AtomicLong(0));
        MismatchReport report = mismatch("c1", "s1", 5, 0,
                lane("10.0.0.1:1", bytes(0, 1, 2, 3, 4, 5, 6, 7), false),
                lane("10.0.0.2:1", bytes(0, 1, 2, 3, 4, 9, 6, 7), true));
        String text = reporter.formatMismatch(report);
        Assert.assertTrue(text.contains("cluster=c1"));
        Assert.assertTrue(text.contains("shard=s1"));
        Assert.assertTrue(text.contains("offset=5"));
        Assert.assertTrue(text.contains("comparedBytes=0"));
        Assert.assertTrue(text.contains("10.0.0.1:1"));
        Assert.assertTrue(text.contains("10.0.0.2:1"));
        Assert.assertTrue(text.contains("minority"));
        Assert.assertTrue(text.contains("0x05="));
        Assert.assertTrue(text.contains("03 04 05 06 07"));
    }

    @Test
    public void testSameShardDumpRateLimitedMismatchCountStillGrows() {
        AtomicLong now = new AtomicLong(1_000);
        RecordingMonitor monitor = new RecordingMonitor();
        DefaultCompareReporter reporter = reporter(monitor, now);
        FakeLane a = new FakeLane("a:1", 0, 16);
        FakeLane b = new FakeLane("b:1", 0, 16);
        byte[] left = bytes(1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3);
        byte[] right = bytes(1, 1, 1, 9, 2, 2, 2, 9, 3, 3, 3, 9);
        a.write(left);
        b.write(right);
        ShardComparator cmp = new ShardComparator("c1", "s1", Arrays.asList(a, b), chunk(4), reporter);

        Assert.assertEquals(CompareOnceResult.MISMATCH, cmp.compareOnce());
        Assert.assertEquals(CompareOnceResult.MISMATCH, cmp.compareOnce());
        Assert.assertEquals(CompareOnceResult.MISMATCH, cmp.compareOnce());
        Assert.assertEquals(3L, cmp.getMismatchCount());
        Assert.assertEquals(1L, reporter.getFullDumpCount());
        Assert.assertEquals(2L, reporter.getSuppressedDumpCount());
        Assert.assertEquals(3, monitor.count(DefaultCompareReporter.EVENT_MISMATCH));

        now.addAndGet(ComparatorConstants.MISMATCH_LOG_MIN_INTERVAL_MILLI);
        a.write(bytes(4, 4, 4, 4));
        b.write(bytes(4, 4, 4, 8));
        Assert.assertEquals(CompareOnceResult.MISMATCH, cmp.compareOnce());
        Assert.assertEquals(4L, cmp.getMismatchCount());
        Assert.assertEquals(2L, reporter.getFullDumpCount());
        Assert.assertEquals(2L, reporter.getSuppressedDumpCount());
    }

    @Test
    public void testForgetShardClearsRateLimit() {
        AtomicLong now = new AtomicLong(1);
        DefaultCompareReporter reporter = reporter(new RecordingMonitor(), now);
        reporter.onMismatch(mismatch("c1", "s1", 0, 0, lane("a:1", bytes(1), false)));
        reporter.onMismatch(mismatch("c1", "s1", 0, 0, lane("a:1", bytes(1), false)));
        Assert.assertEquals(1L, reporter.getFullDumpCount());
        Assert.assertEquals(1L, reporter.getSuppressedDumpCount());
        Assert.assertEquals(1, reporter.getLimitBucketCount());

        reporter.forgetShard("c1", "s1");
        Assert.assertEquals(0, reporter.getLimitBucketCount());
        reporter.onMismatch(mismatch("c1", "s1", 0, 0, lane("a:1", bytes(1), false)));
        Assert.assertEquals(2L, reporter.getFullDumpCount());
        Assert.assertEquals(1L, reporter.getSuppressedDumpCount());
    }

    @Test
    public void testStopTaskClearsDumpLimitBucket() {
        AtomicLong now = new AtomicLong(1_000);
        DefaultCompareReporter reporter = reporter(new RecordingMonitor(), now);
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        try {
            ShardCompareTaskManager manager = new ShardCompareTaskManager(
                    null, null, null, null, new ComparatorConfig(), scheduled,
                    EventMonitor.DEFAULT, reporter);
            manager.putTask(new ShardCompareTask(10L, "c1", "s1"));
            reporter.onMismatch(mismatch("c1", "s1", 0, 0, lane("a:1", bytes(1), false)));
            Assert.assertEquals(1, reporter.getLimitBucketCount());
            manager.stop();
            Assert.assertEquals(0, reporter.getLimitBucketCount());
            reporter.onMismatch(mismatch("c1", "s1", 0, 0, lane("a:1", bytes(1), false)));
            Assert.assertEquals(2L, reporter.getFullDumpCount());
            Assert.assertEquals(0L, reporter.getSuppressedDumpCount());
        } finally {
            scheduled.shutdownNow();
        }
    }

    @Test
    public void testDifferentShardsNotShareLimit() {
        AtomicLong now = new AtomicLong(1);
        DefaultCompareReporter reporter = reporter(new RecordingMonitor(), now);
        reporter.onMismatch(mismatch("c1", "s1", 0, 0, lane("a:1", bytes(1), false)));
        reporter.onMismatch(mismatch("c1", "s2", 0, 0, lane("b:1", bytes(2), false)));
        Assert.assertEquals(2L, reporter.getFullDumpCount());
        Assert.assertEquals(0L, reporter.getSuppressedDumpCount());
    }

    @Test
    public void testEventMonitorThrowDoesNotBreakCallbacks() {
        DefaultCompareReporter reporter = reporter(new ThrowingMonitor(), new AtomicLong(1));
        reporter.onMismatch(mismatch("c1", "s1", 0, 0, lane("a:1", bytes(1), false)));
        reporter.onCompareLost("c1", "s1", 10, 20);
        reporter.onReplIdMismatch("c1", "s1", Arrays.asList("r1", "r2"));
        Assert.assertEquals(1L, reporter.getFullDumpCount());
    }

    @Test
    public void testCompareLostFiresLostAndRealignEvents() {
        RecordingMonitor monitor = new RecordingMonitor();
        DefaultCompareReporter reporter = reporter(monitor, new AtomicLong(1));
        reporter.onCompareLost("c1", "s1", 8, 16);
        Assert.assertEquals(1, monitor.count(DefaultCompareReporter.EVENT_COMPARE_LOST));
        Assert.assertEquals(1, monitor.count(DefaultCompareReporter.EVENT_REALIGN));
        Assert.assertEquals(0, monitor.pairsCalls);
    }

    @Test
    public void testReporterSourceHasNoMetricProxy() throws Exception {
        String text = new String(Files.readAllBytes(
                Paths.get("src/main/java/com/ctrip/xpipe/redis/comparator/report/DefaultCompareReporter.java")),
                StandardCharsets.UTF_8);
        Assert.assertFalse(text.contains("import com.ctrip.xpipe.metric"));
        Assert.assertFalse(text.contains("writeBinMultiDataPoint"));
        Assert.assertFalse(text.contains("nameValuePairs"));
        Assert.assertTrue(text.contains("eventMonitor.logEvent(MONITOR_TYPE, name)"));
    }

    private static DefaultCompareReporter reporter(EventMonitor monitor, AtomicLong now) {
        return new DefaultCompareReporter(new ComparatorConfig() {
            @Override
            public int getMismatchDumpBytes() {
                return 2;
            }
        }, monitor, now::get);
    }

    private static ComparatorConfig chunk(int n) {
        return new ComparatorConfig() {
            @Override
            public int getCompareChunkBytes() {
                return n;
            }
        };
    }

    private static MismatchReport mismatch(String cluster, String shard, long offset, long compared,
                                           LaneBytes... lanes) {
        Map<Byte, List<String>> groups = new LinkedHashMap<>();
        for (LaneBytes lane : lanes) {
            byte value = lane.getChunk().length == 0 ? 0 : lane.getChunk()[Math.min(lane.getChunk().length - 1, 0)];
            if (offset < lane.getChunk().length) {
                value = lane.getChunk()[(int) Math.min(offset, lane.getChunk().length - 1)];
            }
            groups.computeIfAbsent(value, k -> new ArrayList<>()).add(lane.getAddress());
        }
        return new MismatchReport(cluster, shard, offset, compared, Arrays.asList(lanes), groups);
    }

    private static LaneBytes lane(String address, byte[] chunk, boolean minority) {
        return new LaneBytes(address, chunk, minority);
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    static final class FakeLane implements com.ctrip.xpipe.redis.comparator.compare.CompareLane {
        private final String address;
        private final StreamRingBuffer buffer;

        FakeLane(String address, long start, int capacity) {
            this.address = address;
            this.buffer = new StreamRingBuffer(capacity, start);
        }

        void write(byte[] data) {
            buffer.write(Unpooled.wrappedBuffer(data));
        }

        @Override
        public String getAddress() { return address; }

        @Override
        public String getReplId() { return "rid"; }

        @Override
        public long getContinueOffset() { return 0; }

        @Override
        public StreamRingBuffer getBuffer() { return buffer; }

        @Override
        public void disconnect() { }

        @Override
        public void reconnect() { }

        @Override
        public void close() { }
    }

    static final class RecordingMonitor implements EventMonitor {
        final List<String> events = new ArrayList<>();
        int pairsCalls;

        int count(String name) {
            int n = 0;
            for (String e : events) {
                if (e.equals(DefaultCompareReporter.MONITOR_TYPE + "/" + name)) {
                    n++;
                }
            }
            return n;
        }

        @Override
        public void logEvent(String type, String name, long count) {
            events.add(type + "/" + name);
        }

        @Override
        public void logEvent(String type, String name) {
            events.add(type + "/" + name);
        }

        @Override
        public void logEvent(String type, String name, Map<String, String> nameValuePairs) {
            pairsCalls++;
            events.add(type + "/" + name);
        }

        @Override
        public void logError(String type, String name) { }

        @Override
        public void logError(String type, String name, Map<String, String> nameValuePairs) { }

        @Override
        public void logAlertEvent(String simpleAlertMessage) { }
    }

    static final class ThrowingMonitor implements EventMonitor {
        @Override
        public void logEvent(String type, String name, long count) {
            throw new IllegalStateException("event");
        }

        @Override
        public void logEvent(String type, String name) {
            throw new IllegalStateException("event");
        }

        @Override
        public void logEvent(String type, String name, Map<String, String> nameValuePairs) {
            throw new IllegalStateException("event");
        }

        @Override
        public void logError(String type, String name) { }

        @Override
        public void logError(String type, String name, Map<String, String> nameValuePairs) { }

        @Override
        public void logAlertEvent(String simpleAlertMessage) { }
    }
}
