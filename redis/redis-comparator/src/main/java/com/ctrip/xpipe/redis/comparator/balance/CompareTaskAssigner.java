package com.ctrip.xpipe.redis.comparator.balance;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 按 CMS group 的 ciCode 稳定序分配分片（D23 / §4.7 / AC-15）。
 * {@code isMine(shardDbId) = shardDbId % n == idx}。
 * <p>
 * 四条降级路径：① token/url 为空 → {@code n=1, idx=0} + WARN；
 * ② 已配置但不可达 → 保持上次成功分配 + 打点；
 * ③ 首次拉取失败（无历史）→ 不承担任何分片 + ERROR + 打点，禁止兜底 {@code n=1}；
 * ④ {@code idx < 0}（第一段对不上或对上多于一条）→ 不承担任何分片 + ERROR + 打点。
 * <p>
 * 刷新挂 {@link AbstractSpringConfigContext#SCHEDULED_EXECUTOR}，周期与 Meta 相同；
 * 每轮兜 {@code Throwable}。单测注入 {@link ServerGroupProvider}，不访问网络。
 */
public class CompareTaskAssigner {

    public static final String MONITOR_TYPE = "CompareTaskAssigner";

    public static final String EVENT_CMS_UNREACHABLE = "cmsUnreachable";

    public static final String EVENT_CMS_FIRST_FAIL = "cmsFirstFail";

    public static final String EVENT_HOST_NOT_IN_GROUP = "hostNotInGroup";

    public static final char FIRST_LABEL_SEPARATOR = '.';

    private static final Logger logger = LoggerFactory.getLogger(CompareTaskAssigner.class);

    private final ServerGroupProvider provider;

    private final ComparatorConfig config;

    private final FoundationService foundation;

    private final ScheduledExecutorService scheduled;

    private final EventMonitor eventMonitor;

    private final Object refreshLock = new Object();

    private volatile Assignment assignment = Assignment.NONE;

    private volatile Assignment lastSuccess;

    private boolean unconfiguredWarned;

    private int unconfiguredWarnCount;

    private ScheduledFuture<?> refreshFuture;

    public CompareTaskAssigner(ServerGroupProvider provider, ComparatorConfig config,
                               FoundationService foundation, ScheduledExecutorService scheduled) {
        this(provider, config, foundation, scheduled, EventMonitor.DEFAULT);
    }

    public CompareTaskAssigner(ServerGroupProvider provider, ComparatorConfig config,
                               FoundationService foundation, ScheduledExecutorService scheduled,
                               EventMonitor eventMonitor) {
        this.provider = provider;
        this.config = config;
        this.foundation = foundation;
        this.scheduled = scheduled;
        this.eventMonitor = eventMonitor == null ? EventMonitor.DEFAULT : eventMonitor;
    }

    public void start() {
        if (scheduled == null) {
            throw new IllegalStateException(AbstractSpringConfigContext.SCHEDULED_EXECUTOR + " required");
        }
        refreshFuture = scheduled.scheduleWithFixedDelay(this::refreshSafely, 0,
                config.getMetaRefreshIntervalMilli(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        ScheduledFuture<?> future = refreshFuture;
        if (future != null) {
            future.cancel(false);
            refreshFuture = null;
        }
    }

    public boolean isMine(long shardDbId) {
        Assignment current = assignment;
        if (current.n <= 0 || current.idx < 0) {
            return false;
        }
        return shardDbId % current.n == current.idx;
    }

    private void refreshSafely() {
        try {
            refresh();
        } catch (Throwable t) {
            logger.error("[refreshSafely] unexpected", t);
        }
    }

    @VisibleForTesting
    void refresh() {
        synchronized (refreshLock) {
            if (StringUtil.isEmpty(config.getCmsAccessToken()) || StringUtil.isEmpty(config.getCmsGetServerUrl())) {
                applyUnconfiguredFallback();
                return;
            }
            try {
                applyFetched(provider.listCiCodes());
            } catch (Throwable t) {
                if (lastSuccess != null) {
                    logger.warn("[refresh] CMS unreachable, keep last assignment groupId={} host={} n={} idx={}",
                            foundation.getGroupId(), foundation.getHostName(), lastSuccess.n, lastSuccess.idx, t);
                    assignment = lastSuccess;
                    safeLog(EVENT_CMS_UNREACHABLE);
                } else {
                    logger.error("[refresh] CMS first fetch failed, take no shards groupId={} host={}",
                            foundation.getGroupId(), foundation.getHostName(), t);
                    assignment = Assignment.NONE;
                    safeLog(EVENT_CMS_FIRST_FAIL);
                }
            }
        }
    }

    private void applyUnconfiguredFallback() {
        assignment = Assignment.SINGLETON;
        if (!unconfiguredWarned) {
            logger.warn("[refresh] CMS token/url empty, fallback n=1 idx=0 host={}", foundation.getHostName());
            unconfiguredWarned = true;
            unconfiguredWarnCount++;
        }
    }

    private void applyFetched(List<String> rawCodes) {
        List<String> sorted = rawCodes == null ? Collections.emptyList() : rawCodes.stream()
                .filter(code -> !StringUtil.isEmpty(code))
                .map(String::trim)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
        String hostName = foundation.getHostName();
        int idx = indexByFirstLabel(sorted, hostName);
        Assignment next = new Assignment(sorted.size(), idx);
        if (idx < 0) {
            logger.error("[refresh] hostName firstLabel not unique in CMS ciCode list, take no shards groupId={} host={} n={} ciCodes={}",
                    foundation.getGroupId(), hostName, next.n, sorted);
            safeLog(EVENT_HOST_NOT_IN_GROUP);
        } else {
            logger.info("[refresh] assignment groupId={} host={} n={} idx={}",
                    foundation.getGroupId(), hostName, next.n, idx);
        }
        assignment = next;
        lastSuccess = next;
    }

    @VisibleForTesting
    static String firstLabel(String value) {
        if (StringUtil.isEmpty(value)) {
            return "";
        }
        String trimmed = value.trim();
        int dot = trimmed.indexOf(FIRST_LABEL_SEPARATOR);
        return dot < 0 ? trimmed : trimmed.substring(0, dot);
    }

    private static int indexByFirstLabel(List<String> sorted, String hostName) {
        String me = firstLabel(hostName);
        if (me.isEmpty()) {
            return -1;
        }
        int found = -1;
        for (int i = 0; i < sorted.size(); i++) {
            if (!me.equals(firstLabel(sorted.get(i)))) {
                continue;
            }
            if (found >= 0) {
                return -1;
            }
            found = i;
        }
        return found;
    }

    private void safeLog(String name) {
        try {
            eventMonitor.logEvent(MONITOR_TYPE, name);
        } catch (Throwable t) {
            logger.warn("[logEvent] {}", name, t);
        }
    }

    @VisibleForTesting
    int getN() {
        return assignment.n;
    }

    @VisibleForTesting
    int getIdx() {
        return assignment.idx;
    }

    @VisibleForTesting
    int getUnconfiguredWarnCount() {
        return unconfiguredWarnCount;
    }

    static final class Assignment {

        static final Assignment NONE = new Assignment(0, -1);

        static final Assignment SINGLETON = new Assignment(1, 0);

        final int n;

        final int idx;

        Assignment(int n, int idx) {
            this.n = n;
            this.idx = idx;
        }
    }
}
