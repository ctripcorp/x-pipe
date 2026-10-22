package com.ctrip.xpipe.redis.comparator.report;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 比对事件出口（spec D26 / D36 / T-RP.1 / T-RP.2）。
 * <p>
 * 失配 ERROR + hex dump 按分片限流（{@code MISMATCH_LOG_MIN_INTERVAL_MILLI}），
 * 被限流的只跳过完整 dump；{@code mismatchCount} 在 {@code ShardComparator}
 * 回调前已累加，不受限流。CAT 只打 {@code type} + {@code name}，定位进日志。
 * 限流按 {@code cluster/shard} 分桶：每分片一条比对线程，桶内无需加锁。
 * 任务拆除时 {@link #forgetShard} 清桶，避免按「曾经分配过的分片」无限增长。
 * {@code EventMonitor} 抛错只 WARN，不打断比对。
 * 禁止在本类写 {@code MetricProxy}（D36 ②）。
 */
public final class DefaultCompareReporter implements CompareReporter {

    public static final String MONITOR_TYPE = "CompareReporter";

    public static final String EVENT_MISMATCH = "mismatch";

    public static final String EVENT_COMPARE_LOST = "compareLost";

    public static final String EVENT_REALIGN = "realign";

    public static final String EVENT_REPL_ID_MISMATCH = "replIdMismatch";

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private static final Logger logger = LoggerFactory.getLogger(DefaultCompareReporter.class);

    private final int dumpBytes;

    private final EventMonitor eventMonitor;

    private final LongSupplier clock;

    private final Map<String, Long> lastFullDumpAt = new ConcurrentHashMap<>();

    private volatile long fullDumpCount;

    private volatile long suppressedDumpCount;

    public DefaultCompareReporter(ComparatorConfig config) {
        this(config, EventMonitor.DEFAULT, System::currentTimeMillis);
    }

    @VisibleForTesting
    DefaultCompareReporter(ComparatorConfig config, EventMonitor eventMonitor, LongSupplier clock) {
        this.dumpBytes = Objects.requireNonNull(config, "config").getMismatchDumpBytes();
        if (this.dumpBytes < 0) {
            throw new IllegalArgumentException("mismatch.dump.bytes must be >= 0: " + dumpBytes);
        }
        this.eventMonitor = eventMonitor == null ? EventMonitor.DEFAULT : eventMonitor;
        this.clock = clock == null ? System::currentTimeMillis : clock;
    }

    @Override
    public void onMismatch(MismatchReport report) {
        if (report == null) {
            return;
        }
        try {
            if (allowFullDump(report.getCluster(), report.getShard())) {
                fullDumpCount++;
                logger.error("[mismatch] {}", formatMismatch(report));
            } else {
                suppressedDumpCount++;
            }
        } catch (Throwable t) {
            logger.warn("[onMismatch] log cluster={} shard={}", report.getCluster(), report.getShard(), t);
        }
        safeEvent(EVENT_MISMATCH);
    }

    @Override
    public void onCompareLost(String cluster, String shard, long comparedEndBefore, long comparedEndAfter) {
        try {
            logger.info("[compareLost] cluster={} shard={} from={} to={}",
                    cluster, shard, comparedEndBefore, comparedEndAfter);
        } catch (Throwable t) {
            logger.warn("[onCompareLost] log cluster={} shard={}", cluster, shard, t);
        }
        safeEvent(EVENT_COMPARE_LOST);
        safeEvent(EVENT_REALIGN);
    }

    @Override
    public void onReplIdMismatch(String cluster, String shard, List<String> replIds) {
        try {
            logger.warn("[replIdMismatch] cluster={} shard={} replIds={}", cluster, shard, replIds);
        } catch (Throwable t) {
            logger.warn("[onReplIdMismatch] log cluster={} shard={}", cluster, shard, t);
        }
        safeEvent(EVENT_REPL_ID_MISMATCH);
    }

    @Override
    public void forgetShard(String cluster, String shard) {
        if (cluster == null || shard == null) {
            return;
        }
        lastFullDumpAt.remove(cluster + "/" + shard);
    }

    @VisibleForTesting
    long getFullDumpCount() {
        return fullDumpCount;
    }

    @VisibleForTesting
    long getSuppressedDumpCount() {
        return suppressedDumpCount;
    }

    @VisibleForTesting
    int getLimitBucketCount() {
        return lastFullDumpAt.size();
    }

    @VisibleForTesting
    String formatMismatch(MismatchReport report) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("cluster=").append(report.getCluster())
                .append(" shard=").append(report.getShard())
                .append(" offset=").append(report.getMasterReplOffset())
                .append(" comparedBytes=").append(report.getComparedBytes())
                .append(" groups=").append(formatGroups(report.getGroupsByValue()));
        int firstDiff = firstDiff(report.getLanes());
        for (LaneBytes lane : report.getLanes()) {
            sb.append(" [").append(lane.getAddress());
            if (lane.isMinority()) {
                sb.append(" minority");
            }
            sb.append(" hex=").append(hexWindow(lane.getChunk(), firstDiff)).append(']');
        }
        return sb.toString();
    }

    private boolean allowFullDump(String cluster, String shard) {
        String key = cluster + "/" + shard;
        long now = clock.getAsLong();
        Long last = lastFullDumpAt.get(key);
        if (last != null && now - last < ComparatorConstants.MISMATCH_LOG_MIN_INTERVAL_MILLI) {
            return false;
        }
        lastFullDumpAt.put(key, now);
        return true;
    }

    private void safeEvent(String name) {
        try {
            eventMonitor.logEvent(MONITOR_TYPE, name);
        } catch (Throwable t) {
            logger.warn("[logEvent] {}", name, t);
        }
    }

    private static String formatGroups(Map<Byte, List<String>> groups) {
        if (groups == null || groups.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<Byte, List<String>> entry : groups.entrySet()) {
            if (!first) {
                sb.append("; ");
            }
            first = false;
            sb.append("0x").append(HEX[(entry.getKey() >> 4) & 0xf])
                    .append(HEX[entry.getKey() & 0xf])
                    .append('=').append(entry.getValue());
        }
        sb.append('}');
        return sb.toString();
    }

    private static int firstDiff(List<LaneBytes> lanes) {
        if (lanes == null || lanes.isEmpty()) {
            return 0;
        }
        byte[] first = lanes.get(0).getChunk();
        int n = first.length;
        for (int i = 0; i < n; i++) {
            byte b = first[i];
            for (int lane = 1; lane < lanes.size(); lane++) {
                byte[] chunk = lanes.get(lane).getChunk();
                if (i >= chunk.length || chunk[i] != b) {
                    return i;
                }
            }
        }
        return 0;
    }

    private String hexWindow(byte[] chunk, int firstDiff) {
        if (chunk == null || chunk.length == 0) {
            return "";
        }
        int from = Math.max(0, firstDiff - dumpBytes);
        int to = Math.min(chunk.length, firstDiff + dumpBytes + 1);
        return toHex(chunk, from, to);
    }

    @VisibleForTesting
    static String toHex(byte[] chunk, int from, int to) {
        if (from >= to) {
            return "";
        }
        StringBuilder sb = new StringBuilder((to - from) * 3);
        for (int i = from; i < to; i++) {
            if (i > from) {
                sb.append(' ');
            }
            int v = chunk[i] & 0xff;
            sb.append(HEX[v >>> 4]).append(HEX[v & 0xf]);
        }
        return sb.toString();
    }
}
