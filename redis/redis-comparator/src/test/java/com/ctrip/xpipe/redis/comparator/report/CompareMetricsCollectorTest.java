package com.ctrip.xpipe.redis.comparator.report;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager.ShardCompareTask;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class CompareMetricsCollectorTest extends AbstractTest {

    /** AC-22 ①（v1.82）：每分片只写 comparedBytes + mismatch 两点；无比对器的任务不写。 */
    @Test
    public void testWritesComparedBytesAndMismatchOnly() {
        RecordingProxy proxy = new RecordingProxy();
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        ShardCompareTaskManager manager = emptyManager(scheduled);
        try {
            manager.putTask(comparedTask("c1", "s1", 10L));
            manager.putTask(new ShardCompareTask(11L, "c2", "s2"));

            CompareMetricsCollector collector = new CompareMetricsCollector(manager,
                    scheduled, new ComparatorConfig(), "jq", () -> proxy);
            collector.reportOnce();

            Assert.assertEquals(2, proxy.points.size());
            MetricData compared = proxy.points.get(0);
            Assert.assertEquals(CompareMetricsCollector.METRIC_COMPARED_BYTES, compared.getMetricType());
            Assert.assertEquals(4.0, compared.getValue(), 0.0);
            assertTags(compared, "c1", "s1");
            MetricData mismatch = proxy.points.get(1);
            Assert.assertEquals(CompareMetricsCollector.METRIC_MISMATCH, mismatch.getMetricType());
            Assert.assertEquals(0.0, mismatch.getValue(), 0.0);
            assertTags(mismatch, "c1", "s1");
        } finally {
            scheduled.shutdownNow();
        }
    }

    /** AC-22 ①（v1.82）：mismatch 上报累计值，下一轮无新失配时不归零、不做差。 */
    @Test
    public void testMismatchReportsCumulativeValue() {
        RecordingProxy proxy = new RecordingProxy();
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        ShardCompareTaskManager manager = emptyManager(scheduled);
        try {
            ShardCompareTask task = mismatchedTask("c1", "s1", 10L);
            manager.putTask(task);
            CompareMetricsCollector collector = new CompareMetricsCollector(manager,
                    scheduled, new ComparatorConfig(), "jq", () -> proxy);

            collector.reportOnce();
            collector.reportOnce();

            List<Double> values = new ArrayList<>();
            for (MetricData point : proxy.points) {
                if (CompareMetricsCollector.METRIC_MISMATCH.equals(point.getMetricType())) {
                    values.add(point.getValue());
                }
            }
            Assert.assertEquals(Arrays.asList(1.0, 1.0), values);
            Assert.assertEquals(1L, task.getComparator().getMismatchCount());
        } finally {
            scheduled.shutdownNow();
        }
    }

    @Test
    public void testMetricProxyThrowDoesNotPropagate() {
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        try {
            ShardCompareTaskManager manager = emptyManager(scheduled);
            manager.putTask(comparedTask("c1", "s1", 10L));
            ThrowingProxy proxy = new ThrowingProxy();
            CompareMetricsCollector collector = new CompareMetricsCollector(manager,
                    scheduled, new ComparatorConfig(), "jq", () -> proxy);
            collector.reportOnce();
            // comparedBytes 抛错后 mismatch 仍照写：两点各自兜异常
            Assert.assertEquals(2, proxy.writes);
        } finally {
            scheduled.shutdownNow();
        }
    }

    @Test
    public void testMissingHickwallFallsBackWithoutThrow() {
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        try {
            ShardCompareTaskManager manager = emptyManager(scheduled);
            CompareMetricsCollector collector = new CompareMetricsCollector(manager,
                    scheduled, new ComparatorConfig(), "jq",
                    () -> {
                        throw new IllegalStateException("service not found: MetricProxy");
                    });
            collector.reportOnce();
            collector.reportOnce();
        } finally {
            scheduled.shutdownNow();
        }
    }

    @Test
    public void testCollectorIsOnlyMetricWriteSite() throws Exception {
        String loop = new String(Files.readAllBytes(
                Paths.get("src/main/java/com/ctrip/xpipe/redis/comparator/compare/ShardComparator.java")),
                StandardCharsets.UTF_8);
        Assert.assertFalse(loop.contains("import com.ctrip.xpipe.metric"));
        Assert.assertFalse(loop.contains("writeBinMultiDataPoint"));
        String reporter = new String(Files.readAllBytes(
                Paths.get("src/main/java/com/ctrip/xpipe/redis/comparator/report/DefaultCompareReporter.java")),
                StandardCharsets.UTF_8);
        Assert.assertFalse(reporter.contains("import com.ctrip.xpipe.metric"));
        Assert.assertFalse(reporter.contains("writeBinMultiDataPoint"));
        String collector = new String(Files.readAllBytes(
                Paths.get("src/main/java/com/ctrip/xpipe/redis/comparator/report/CompareMetricsCollector.java")),
                StandardCharsets.UTF_8);
        Assert.assertTrue(collector.contains("writeBinMultiDataPoint"));
        Assert.assertTrue(collector.contains("SCHEDULED_EXECUTOR"));
        Assert.assertTrue(collector.contains("scheduleWithFixedDelay"));
        Assert.assertFalse(collector.contains("logger.error"));
        Assert.assertFalse(collector.contains("mismatchCount"));
        Assert.assertFalse(collector.contains("compareLost"));
        Assert.assertFalse(collector.contains("realignCount"));
        Assert.assertFalse(collector.contains("streamReconnectCount"));
        Assert.assertFalse(collector.contains("replIdMismatch"));
        Assert.assertFalse(collector.contains("shardsAssigned"));
        Assert.assertFalse(collector.contains("streamsRunning"));
        Assert.assertFalse(collector.contains("lagBytes"));
    }

    private static ShardCompareTask comparedTask(String cluster, String shard, long dbId) {
        FakeLane a = new FakeLane("10.0.0.1:6380", 100, 32, 2);
        FakeLane b = new FakeLane("10.0.0.2:6380", 100, 32, 1);
        a.write(new byte[]{1, 2, 3, 4});
        b.write(new byte[]{1, 2, 3, 4});
        ShardComparator cmp = new ShardComparator(cluster, shard, Arrays.asList(a, b),
                new ComparatorConfig(), CompareReporter.NOOP);
        Assert.assertEquals(ShardComparator.CompareOnceResult.ADVANCED, cmp.compareOnce());
        ShardCompareTask task = new ShardCompareTask(dbId, cluster, shard);
        Map<String, CompareLane> streams = new LinkedHashMap<>();
        streams.put(a.getAddress(), a);
        streams.put(b.getAddress(), b);
        task.bind(cmp, streams);
        return task;
    }

    private static ShardCompareTask mismatchedTask(String cluster, String shard, long dbId) {
        FakeLane a = new FakeLane("10.0.0.1:6380", 100, 32, 0);
        FakeLane b = new FakeLane("10.0.0.2:6380", 100, 32, 0);
        a.write(new byte[]{1, 2, 3, 4});
        b.write(new byte[]{1, 2, 9, 4});
        ShardComparator cmp = new ShardComparator(cluster, shard, Arrays.asList(a, b),
                new ComparatorConfig(), CompareReporter.NOOP);
        Assert.assertEquals(ShardComparator.CompareOnceResult.MISMATCH, cmp.compareOnce());
        ShardCompareTask task = new ShardCompareTask(dbId, cluster, shard);
        Map<String, CompareLane> streams = new LinkedHashMap<>();
        streams.put(a.getAddress(), a);
        streams.put(b.getAddress(), b);
        task.bind(cmp, streams);
        return task;
    }

    private static void assertTags(MetricData point, String cluster, String shard) {
        Assert.assertEquals(cluster, point.getClusterName());
        Assert.assertEquals(shard, point.getShardName());
        Assert.assertEquals("jq", point.getDcName());
    }

    private static ShardCompareTaskManager emptyManager(ScheduledExecutorService scheduled) {
        return new ShardCompareTaskManager(null, null, null, null, new ComparatorConfig(), scheduled);
    }

    static final class FakeLane implements CompareLane {
        private final String address;
        private final StreamRingBuffer buffer;
        private final int reconnect;

        FakeLane(String address, long start, int capacity, int reconnect) {
            this.address = address;
            this.buffer = new StreamRingBuffer(capacity, start);
            this.reconnect = reconnect;
        }

        void write(byte[] data) {
            buffer.write(Unpooled.wrappedBuffer(data));
        }

        @Override
        public String getAddress() { return address; }

        @Override
        public String getReplId() { return "rid"; }

        @Override
        public long getContinueOffset() { return 100; }

        @Override
        public StreamRingBuffer getBuffer() { return buffer; }

        @Override
        public void disconnect() { }

        @Override
        public void reconnect() { }

        @Override
        public void close() { }

        @Override
        public int getStreamReconnectCount() { return reconnect; }
    }

    static final class RecordingProxy implements MetricProxy {
        final List<MetricData> points = new ArrayList<>();

        @Override
        public void writeBinMultiDataPoint(MetricData data) {
            points.add(data);
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }

    static final class ThrowingProxy implements MetricProxy {
        int writes;

        @Override
        public void writeBinMultiDataPoint(MetricData data) throws MetricProxyException {
            writes++;
            throw new MetricProxyException("boom");
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }
}
