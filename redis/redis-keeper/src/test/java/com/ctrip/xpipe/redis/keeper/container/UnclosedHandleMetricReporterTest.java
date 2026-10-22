package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleCloser;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * keeper-tfs-m6 P8 (spec §8): the two unclosed-handle gauges are reported every minute, zero included.
 */
public class UnclosedHandleMetricReporterTest {

    private static final ReplId REPL_ID = ReplId.from(1L);

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private final List<MetricData> points = new ArrayList<>();
    private ScheduledExecutorService scheduled;
    private DeferredHandleCloser closer;

    @Before
    public void setUp() {
        scheduled = mock(ScheduledExecutorService.class);
        closer = DeferredHandleTestSupport.idleCloser(mock(AsyncFileSystem.class), clock::get, 300_000L);
    }

    @Test
    public void testReportsBothGaugesEveryMinuteIncludingZero() {
        UnclosedHandleMetricReporter reporter =
                new UnclosedHandleMetricReporter(closer, new RecordingProxy(points, 0), scheduled, clock::get);
        reporter.start();
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduled).scheduleAtFixedRate(task.capture(), eq(60_000L), eq(60_000L), eq(TimeUnit.MILLISECONDS));

        task.getValue().run();
        assertPoints(0, 0);

        DeferredHandleTestSupport.deferFile(closer, "/data/repl-1/meta.v2.json", true, REPL_ID, "close meta",
                new IOException("eio"));
        DeferredHandleTestSupport.deferSegment(closer, "/data/repl-1", "cmd_", false, REPL_ID, "close reader",
                new IOException("eio"));
        points.clear();
        clock.addAndGet(60_000L);
        task.getValue().run();
        assertPoints(2, 1);

        reporter.stop();
        verify(scheduled).shutdownNow();
    }

    @Test
    public void testWriteFailureOnlyWarnsAndOtherGaugeIsStillSent() {
        RecordingProxy failingFirst = new RecordingProxy(points, 1);
        UnclosedHandleMetricReporter reporter =
                new UnclosedHandleMetricReporter(closer, failingFirst, scheduled, clock::get);
        reporter.report();
        assertEquals(2, failingFirst.writes);
        assertEquals("the second gauge is sent after the first one failed", 1, points.size());
        assertEquals(UnclosedHandleMetricReporter.METRIC_WRITE_COUNT, points.get(0).getMetricType());

        // Even a closer failure must not escape the periodic task.
        UnclosedHandleMetricReporter broken = new UnclosedHandleMetricReporter(null, failingFirst, scheduled,
                clock::get);
        broken.report();
    }

    /** Records points; the first {@code failures} writes throw. */
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

    private void assertPoints(int count, int writeCount) {
        assertEquals(2, points.size());
        assertEquals(UnclosedHandleMetricReporter.METRIC_COUNT, points.get(0).getMetricType());
        assertEquals(count, points.get(0).getValue(), 0.0);
        assertEquals(UnclosedHandleMetricReporter.METRIC_WRITE_COUNT, points.get(1).getMetricType());
        assertEquals(writeCount, points.get(1).getValue(), 0.0);
        for (MetricData point : points) {
            assertEquals(clock.get(), point.getTimestampMilli());
        }
    }
}
