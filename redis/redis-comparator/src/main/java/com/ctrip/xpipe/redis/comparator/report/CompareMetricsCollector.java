package com.ctrip.xpipe.redis.comparator.report;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.metric.DummyMetricProxy;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager.ShardCompareTask;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 周期指标出口（spec D36 ② / §4.8.3 / T-RP.3）。
 * <p>
 * 挂 {@link AbstractSpringConfigContext#SCHEDULED_EXECUTOR}，每轮兜 {@code Throwable}。
 * Hickwall <b>只</b>打 {@code comparedBytes}；其它计数进 CAT Event 与 {@code /api/status}。
 * 禁止在比对循环 / {@link CompareReporter} 回调里逐次 write。
 * {@code MetricProxy} 不可用（非携程、无 Hickwall）降级为 {@link DummyMetricProxy}，
 * 只 WARN，不抛、不刷 ERROR。
 */
public class CompareMetricsCollector {

    public static final String METRIC_COMPARED_BYTES = "comparedBytes";

    private static final Logger logger = LoggerFactory.getLogger(CompareMetricsCollector.class);

    private static final DummyMetricProxy FALLBACK = new DummyMetricProxy();

    private final ShardCompareTaskManager taskManager;

    private final ScheduledExecutorService scheduled;

    private final ComparatorConfig config;

    private final String dcName;

    private final Supplier<MetricProxy> metricProxySupplier;

    private volatile MetricProxy metricProxy;

    private ScheduledFuture<?> future;

    public CompareMetricsCollector(ShardCompareTaskManager taskManager,
                                   ScheduledExecutorService scheduled,
                                   ComparatorConfig config) {
        this(taskManager, scheduled, config, FoundationService.DEFAULT.getDataCenter(),
                CompareMetricsCollector::loadMetricProxy);
    }

    @VisibleForTesting
    CompareMetricsCollector(ShardCompareTaskManager taskManager, ScheduledExecutorService scheduled,
                            ComparatorConfig config, String dcName, Supplier<MetricProxy> metricProxySupplier) {
        this.taskManager = Objects.requireNonNull(taskManager, "taskManager");
        this.scheduled = Objects.requireNonNull(scheduled, "scheduled");
        this.config = Objects.requireNonNull(config, "config");
        this.dcName = dcName == null ? "" : dcName;
        this.metricProxySupplier = metricProxySupplier == null
                ? CompareMetricsCollector::loadMetricProxy : metricProxySupplier;
    }

    public void start() {
        if (scheduled == null) {
            throw new IllegalStateException(AbstractSpringConfigContext.SCHEDULED_EXECUTOR + " required");
        }
        int interval = Math.max(config.getMetaRefreshIntervalMilli(), 1);
        future = scheduled.scheduleWithFixedDelay(this::reportSafely, 0, interval, TimeUnit.MILLISECONDS);
        logger.info("[start] intervalMilli={}", interval);
    }

    public void stop() {
        ScheduledFuture<?> toCancel = future;
        if (toCancel != null) {
            toCancel.cancel(false);
            future = null;
        }
    }

    private void reportSafely() {
        try {
            reportOnce();
        } catch (Throwable t) {
            logger.warn("[reportSafely] unexpected", t);
        }
    }

    @VisibleForTesting
    void reportOnce() {
        MetricProxy proxy = resolveProxy();
        Map<Long, ShardCompareTask> tasks = taskManager.getTasks();
        long now = System.currentTimeMillis();
        for (ShardCompareTask task : tasks.values()) {
            writeComparedBytes(proxy, task, now);
        }
    }

    private void writeComparedBytes(MetricProxy proxy, ShardCompareTask task, long now) {
        ShardComparator cmp = task.getComparator();
        if (cmp == null) {
            return;
        }
        MetricData data = new MetricData(METRIC_COMPARED_BYTES, dcName, cmp.getCluster(), cmp.getShard());
        data.setTimestampMilli(now);
        data.setValue(cmp.getComparedBytes());
        write(proxy, data);
    }

    private void write(MetricProxy proxy, MetricData data) {
        try {
            proxy.writeBinMultiDataPoint(data);
        } catch (Throwable t) {
            logger.warn("[write] {}", data.getMetricType(), t);
        }
    }

    private MetricProxy resolveProxy() {
        MetricProxy cached = metricProxy;
        if (cached != null) {
            return cached;
        }
        try {
            cached = metricProxySupplier.get();
        } catch (Throwable t) {
            logger.warn("[metricProxy] unavailable, fallback dummy", t);
            cached = FALLBACK;
        }
        if (cached == null) {
            cached = FALLBACK;
        }
        metricProxy = cached;
        return cached;
    }

    private static MetricProxy loadMetricProxy() {
        return ServicesUtil.getMetricProxy();
    }
}
