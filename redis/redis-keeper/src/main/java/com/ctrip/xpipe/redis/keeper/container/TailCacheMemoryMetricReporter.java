package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheMemoryStats;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Reports container TailCache memory every minute, zero included. Skipped while the FS is not created.
 */
public class TailCacheMemoryMetricReporter {

    private static final Logger logger = LoggerFactory.getLogger(TailCacheMemoryMetricReporter.class);

    static final String METRIC_MAX = "keeper.tailcache.max.bytes";
    static final String METRIC_COMMITTED = "keeper.tailcache.committed.bytes";
    static final String METRIC_UNDURABLE = "keeper.tailcache.undurable.bytes";
    static final long REPORT_INTERVAL_MILLIS = 60_000L;
    private static final String THREAD_NAME = "keeper-tailcache-memory-metric";

    private final Supplier<Optional<TailCacheMemoryStats>> statsSupplier;
    private final MetricProxy metricProxy;
    private final ScheduledExecutorService scheduled;
    private final LongSupplier clock;

    public TailCacheMemoryMetricReporter(Supplier<Optional<TailCacheMemoryStats>> statsSupplier) {
        this(statsSupplier, MetricProxy.DEFAULT, System::currentTimeMillis);
    }

    TailCacheMemoryMetricReporter(Supplier<Optional<TailCacheMemoryStats>> statsSupplier, MetricProxy metricProxy,
                                  LongSupplier clock) {
        this.statsSupplier = statsSupplier;
        this.metricProxy = metricProxy;
        this.clock = clock;
        this.scheduled = Executors.newSingleThreadScheduledExecutor(XpipeThreadFactory.create(THREAD_NAME, true));
    }

    boolean isStopped() {
        return scheduled.isShutdown();
    }

    public void start() {
        scheduled.scheduleAtFixedRate(this::report, REPORT_INTERVAL_MILLIS, REPORT_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    // Idempotent.
    public void stop() {
        scheduled.shutdownNow();
    }

    // Never throws, so the periodic task keeps running.
    void report() {
        try {
            Optional<TailCacheMemoryStats> stats = statsSupplier.get();
            if (!stats.isPresent()) {
                return;
            }
            long now = clock.getAsLong();
            send(METRIC_MAX, stats.get().getMaxCacheSizeBytes(), now);
            send(METRIC_COMMITTED, stats.get().getCommittedBytes(), now);
            send(METRIC_UNDURABLE, stats.get().getUnDurableBytes(), now);
        } catch (Throwable t) {
            logger.warn("[tailcache-memory-metric] report failed", t);
        }
    }

    private void send(String type, long value, long now) {
        MetricData data = new MetricData(type);
        data.setValue(value);
        data.setTimestampMilli(now);
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Throwable t) {
            logger.warn("[tailcache-memory-metric] write {}={} failed", type, value, t);
        }
    }
}
