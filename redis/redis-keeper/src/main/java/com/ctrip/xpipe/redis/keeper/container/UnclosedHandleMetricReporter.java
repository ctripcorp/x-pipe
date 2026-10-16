package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleCloser;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Reports the container's pending deferred-close handles to Hickwall every minute, zero included
 * (keeper-tfs-m6 spec §8). Runs on its own thread: the closer thread blocks on closes and would
 * delay the report exactly when handles pile up.
 */
public class UnclosedHandleMetricReporter {

    private static final Logger logger = LoggerFactory.getLogger(UnclosedHandleMetricReporter.class);

    static final String METRIC_COUNT = "keeper.unclosed.handle.count";
    static final String METRIC_WRITE_COUNT = "keeper.unclosed.handle.write.count";
    static final long REPORT_INTERVAL_MILLIS = 60_000L;
    private static final String THREAD_NAME = "keeper-unclosed-handle-metric";

    private final DeferredHandleCloser closer;
    private final MetricProxy metricProxy;
    private final ScheduledExecutorService scheduled;
    private final LongSupplier clock;

    public UnclosedHandleMetricReporter(DeferredHandleCloser closer) {
        this(closer, MetricProxy.DEFAULT,
                Executors.newSingleThreadScheduledExecutor(XpipeThreadFactory.create(THREAD_NAME, true)),
                System::currentTimeMillis);
    }

    UnclosedHandleMetricReporter(DeferredHandleCloser closer, MetricProxy metricProxy,
                                 ScheduledExecutorService scheduled, LongSupplier clock) {
        this.closer = closer;
        this.metricProxy = metricProxy;
        this.scheduled = scheduled;
        this.clock = clock;
    }

    public void start() {
        scheduled.scheduleAtFixedRate(this::report, REPORT_INTERVAL_MILLIS, REPORT_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduled.shutdownNow();
    }

    /** Never throws, so the periodic task keeps running. */
    void report() {
        try {
            long now = clock.getAsLong();
            send(METRIC_COUNT, closer.count(), now);
            send(METRIC_WRITE_COUNT, closer.writeCount(), now);
        } catch (Throwable t) {
            logger.warn("[unclosed-handle-metric] report failed", t);
        }
    }

    private void send(String type, int value, long now) {
        MetricData data = new MetricData(type);
        data.setValue(value);
        data.setTimestampMilli(now);
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Throwable t) {
            logger.warn("[unclosed-handle-metric] write {}={} failed", type, value, t);
        }
    }
}
