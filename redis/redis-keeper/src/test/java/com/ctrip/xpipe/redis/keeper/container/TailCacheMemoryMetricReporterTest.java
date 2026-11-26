package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheMemoryStats;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class TailCacheMemoryMetricReporterTest {

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private final List<MetricData> points = new ArrayList<>();
    private final AtomicReference<Optional<TailCacheMemoryStats>> stats = new AtomicReference<>(Optional.empty());
    private final List<TailCacheMemoryMetricReporter> reporters = new ArrayList<>();

    @After
    public void tearDown() {
        reporters.forEach(TailCacheMemoryMetricReporter::stop);
    }

    @Test
    public void testReportsThreeGaugesEveryMinuteIncludingZero() {
        assertEquals(60_000L, TailCacheMemoryMetricReporter.REPORT_INTERVAL_MILLIS);
        TailCacheMemoryMetricReporter reporter = newReporter(new RecordingProxy(points, 0));

        stats.set(Optional.of(new TailCacheMemoryStats(0, 1024, 0, 0)));
        reporter.report();
        assertPoints(1024, 0, 0);

        points.clear();
        clock.addAndGet(60_000L);
        stats.set(Optional.of(new TailCacheMemoryStats(0, 1024, 512, 100)));
        reporter.report();
        assertPoints(1024, 512, 100);
    }

    @Test
    public void testSkipsWhenFsNotCreated() {
        newReporter(new RecordingProxy(points, 0)).report();
        assertTrue(points.isEmpty());
    }

    @Test
    public void testWriteFailureDoesNotStopOtherGauges() {
        stats.set(Optional.of(new TailCacheMemoryStats(0, 1024, 512, 100)));
        RecordingProxy failingFirst = new RecordingProxy(points, 1);
        newReporter(failingFirst).report();
        assertEquals(3, failingFirst.writes);
        assertEquals(2, points.size());
        assertEquals(TailCacheMemoryMetricReporter.METRIC_COMMITTED, points.get(0).getMetricType());
        assertEquals(TailCacheMemoryMetricReporter.METRIC_UNDURABLE, points.get(1).getMetricType());
    }

    @Test
    public void testStatsFailureDoesNotEscape() {
        TailCacheMemoryMetricReporter reporter = new TailCacheMemoryMetricReporter(() -> {
            throw new IllegalStateException("broken");
        }, new RecordingProxy(points, 0), clock::get);
        reporters.add(reporter);
        reporter.report();
        assertTrue(points.isEmpty());
    }

    @Test
    public void testStopIsIdempotent() {
        TailCacheMemoryMetricReporter reporter = newReporter(new RecordingProxy(points, 0));
        reporter.start();
        assertFalse(reporter.isStopped());
        reporter.stop();
        reporter.stop();
        assertTrue(reporter.isStopped());
    }

    private TailCacheMemoryMetricReporter newReporter(MetricProxy proxy) {
        TailCacheMemoryMetricReporter reporter = new TailCacheMemoryMetricReporter(stats::get, proxy, clock::get);
        reporters.add(reporter);
        return reporter;
    }

    private void assertPoints(long max, long committed, long unDurable) {
        assertEquals(3, points.size());
        assertPoint(points.get(0), TailCacheMemoryMetricReporter.METRIC_MAX, max);
        assertPoint(points.get(1), TailCacheMemoryMetricReporter.METRIC_COMMITTED, committed);
        assertPoint(points.get(2), TailCacheMemoryMetricReporter.METRIC_UNDURABLE, unDurable);
    }

    private void assertPoint(MetricData point, String type, long value) {
        assertEquals(type, point.getMetricType());
        assertEquals(value, point.getValue(), 0.0);
        assertEquals(clock.get(), point.getTimestampMilli());
    }

    // The first {@code failures} writes throw.
    private static final class RecordingProxy implements MetricProxy {
        private final List<MetricData> points;
        private final int failures;
        int writes;

        RecordingProxy(List<MetricData> points, int failures) {
            this.points = points;
            this.failures = failures;
        }

        @Override
        public void writeBinMultiDataPoint(MetricData data) throws MetricProxyException {
            if (++writes <= failures) {
                throw new MetricProxyException("down");
            }
            points.add(data);
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }
}
