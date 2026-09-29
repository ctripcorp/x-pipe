package com.ctrip.xpipe.redis.comparator.meta;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.comparator.balance.CompareTaskAssigner;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DcMeta → 分片比对任务，增量启停（D22 / D35 ① / §4.6.2 / D33 ⑤）。
 * <p>
 * 同步策略：任务表只在 {@code refreshLock} 下改（周期 refresh 与 probe 回调经
 * 专用 {@code comparatorTaskScheduled#execute} 回流，与 ACK/reconnect 隔离）。
 * {@code stop()} 锁内置 {@code stopped} 并抬 {@code refreshEpoch}。
 * {@code stopped} 只在 {@code refreshLock} 内读写：{@code refresh} 进锁后、
 * {@code ++refreshEpoch} 之前看见则返回；{@code applyProbed} 见停止位或
 * epoch 不匹配则返回。拆任务时 {@link KeeperStreamFactory#release(long)}。
 * 协商异步扇出，禁止 for 循环阻塞连 Keeper。未变化的 {@code ip:port} 不重建连接。
 * keeper 增删 / 端口变：关掉离开的 {@link CompareLane}、open 新实例，
 * {@link ShardComparator#replaceLanes} 整数组替换，不重建比对线程。
 * 先 {@code new ShardComparator} + {@code start}，再
 * {@code open(..., cmp::wake)}（绑死该实例，不用 {@code task.comparator} 间接层），
 * 然后 {@code replaceLanes}。open 成功不足 2 路：先 {@code stop} 比对器，再 close lane、拆任务。
 * 同实例增删 keeper：新路仍绑当前 {@code cmp::wake}，离开的路先 close 再 {@code replaceLanes}。
 * 拆任务 {@code stop()} 比对器，再 {@code close} 本 Manager 打开的全部 lane，
 * 并 {@link CompareReporter#forgetShard} 清掉该分片的 dump 限流桶。
 * 任务增删在专用 scheduled 上起停本分片比对线程（D33 ⑤⑥）；超
 * {@code COMPARE_THREAD_WARN_THRESHOLD} 只 WARN + 打点，不拒绝建任务。
 */
public class ShardCompareTaskManager {

    public static final String MONITOR_TYPE = "ShardCompareTaskManager";

    public static final String EVENT_INSUFFICIENT_LANES = "insufficientLanes";

    public static final String EVENT_COMPARE_THREAD_WARN = "compareThreadWarn";

    public static final String TASK_SCHEDULED = "comparatorTaskScheduled";

    private static final Logger logger = LoggerFactory.getLogger(ShardCompareTaskManager.class);

    private final ComparatorMetaService metaService;

    private final CompareTaskAssigner assigner;

    private final PrepareWatchCache watchCache;

    private final KeeperStreamFactory streamFactory;

    private final ComparatorConfig config;

    private final ScheduledExecutorService scheduled;

    private final EventMonitor eventMonitor;

    private final CompareReporter reporter;

    private int compareThreadWarnThreshold = ComparatorConstants.COMPARE_THREAD_WARN_THRESHOLD;

    private final Object refreshLock = new Object();

    private final Map<Long, ShardCompareTask> tasks = new LinkedHashMap<>();

    private long refreshEpoch;

    private boolean stopped;

    private ScheduledFuture<?> refreshFuture;

    public ShardCompareTaskManager(ComparatorMetaService metaService, CompareTaskAssigner assigner,
                                   PrepareWatchCache watchCache, KeeperStreamFactory streamFactory,
                                   ComparatorConfig config, ScheduledExecutorService scheduled) {
        this(metaService, assigner, watchCache, streamFactory, config, scheduled, EventMonitor.DEFAULT);
    }

    public ShardCompareTaskManager(ComparatorMetaService metaService, CompareTaskAssigner assigner,
                                   PrepareWatchCache watchCache, KeeperStreamFactory streamFactory,
                                   ComparatorConfig config, ScheduledExecutorService scheduled,
                                   EventMonitor eventMonitor) {
        this(metaService, assigner, watchCache, streamFactory, config, scheduled, eventMonitor,
                CompareReporter.NOOP);
    }

    public ShardCompareTaskManager(ComparatorMetaService metaService, CompareTaskAssigner assigner,
                                   PrepareWatchCache watchCache, KeeperStreamFactory streamFactory,
                                   ComparatorConfig config, ScheduledExecutorService scheduled,
                                   EventMonitor eventMonitor, CompareReporter reporter) {
        this.metaService = metaService;
        this.assigner = assigner;
        this.watchCache = watchCache;
        this.streamFactory = streamFactory;
        this.config = config;
        this.scheduled = scheduled;
        this.eventMonitor = eventMonitor == null ? EventMonitor.DEFAULT : eventMonitor;
        this.reporter = reporter == null ? CompareReporter.NOOP : reporter;
    }

    public void start() {
        if (scheduled == null) {
            throw new IllegalStateException(TASK_SCHEDULED + " required");
        }
        synchronized (refreshLock) {
            stopped = false;
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
        synchronized (refreshLock) {
            stopped = true;
            refreshEpoch++;
            List<ShardCompareTask> snapshot = new ArrayList<>(tasks.values());
            for (ShardCompareTask task : snapshot) {
                stopTask(task);
            }
        }
    }

    public Map<Long, ShardCompareTask> getTasks() {
        synchronized (refreshLock) {
            return Collections.unmodifiableMap(new LinkedHashMap<>(tasks));
        }
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
        DcMeta dcMeta;
        try {
            dcMeta = metaService.getCurrentDcMeta();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        Map<Long, KeeperContainerMeta> containers = metaService.indexKeeperContainers(dcMeta);
        watchCache.invalidateAll();
        Map<Long, DesiredShard> desired = collectDesired(dcMeta, containers);
        long epoch;
        synchronized (refreshLock) {
            if (stopped) {
                return;
            }
            epoch = ++refreshEpoch;
            List<ShardCompareTask> gone = new ArrayList<>();
            for (ShardCompareTask task : tasks.values()) {
                if (!desired.containsKey(task.dbId)) {
                    gone.add(task);
                }
            }
            for (ShardCompareTask task : gone) {
                logger.info("[removeTask] cluster={} shard={} dbId={}", task.cluster, task.shard, task.dbId);
                stopTask(task);
            }
        }
        for (DesiredShard shard : desired.values()) {
            probeAndApply(epoch, shard);
        }
    }

    private Map<Long, DesiredShard> collectDesired(DcMeta dcMeta, Map<Long, KeeperContainerMeta> containers) {
        Map<Long, DesiredShard> desired = new LinkedHashMap<>();
        if (dcMeta == null || dcMeta.getClusters() == null) {
            return desired;
        }
        for (ClusterMeta cluster : dcMeta.getClusters().values()) {
            if (cluster == null || cluster.getShards() == null) {
                continue;
            }
            String clusterId = cluster.getId();
            for (ShardMeta shard : cluster.getShards().values()) {
                if (shard == null) {
                    continue;
                }
                Long dbId = shard.getDbId();
                if (dbId == null) {
                    logger.warn("[refresh] skip shard without dbId, cluster={} shard={}", clusterId, shard.getId());
                    continue;
                }
                List<KeeperMeta> tfsKeepers = metaService.listTfsKeepers(shard, containers);
                if (tfsKeepers.size() < 2) {
                    continue;
                }
                if (!assigner.isMine(dbId)) {
                    continue;
                }
                DesiredShard desiredShard = new DesiredShard(dbId, clusterId, shard.getId());
                for (KeeperMeta keeper : tfsKeepers) {
                    Endpoint endpoint = endpointOf(keeper);
                    if (endpoint == null) {
                        continue;
                    }
                    desiredShard.keepers.putIfAbsent(addressOf(endpoint), endpoint);
                }
                if (desiredShard.keepers.size() < 2) {
                    logger.warn("[refresh] skip shard with <2 addressed TFS keepers, cluster={} shard={} dbId={}",
                            clusterId, shard.getId(), dbId);
                    continue;
                }
                desired.put(dbId, desiredShard);
            }
        }
        return desired;
    }

    private void probeAndApply(long epoch, DesiredShard desired) {
        Map<String, Boolean> ready = new ConcurrentHashMap<>();
        AtomicInteger pending = new AtomicInteger(desired.keepers.size());
        for (Map.Entry<String, Endpoint> entry : desired.keepers.entrySet()) {
            String key = entry.getKey();
            try {
                CommandFuture<Boolean> future = watchCache.query(entry.getValue());
                future.addListener(f -> onProbe(epoch, desired, ready, pending, key, f));
            } catch (Throwable t) {
                logger.warn("[prepareWatch] cluster={} shard={} keeper={}", desired.cluster, desired.shard, key, t);
                onProbeDone(epoch, desired, ready, pending, key, false);
            }
        }
    }

    private void onProbe(long epoch, DesiredShard desired, Map<String, Boolean> ready, AtomicInteger pending,
                         String key, CommandFuture<Boolean> future) {
        boolean ok = false;
        try {
            ok = future.isSuccess() && Boolean.TRUE.equals(future.getNow());
        } catch (Throwable t) {
            logger.warn("[prepareWatch] cluster={} shard={} keeper={}", desired.cluster, desired.shard, key, t);
        }
        onProbeDone(epoch, desired, ready, pending, key, ok);
    }

    private void onProbeDone(long epoch, DesiredShard desired, Map<String, Boolean> ready, AtomicInteger pending,
                             String key, boolean ok) {
        ready.put(key, ok);
        if (pending.decrementAndGet() != 0) {
            return;
        }
        scheduled.execute(() -> {
            try {
                applyProbed(epoch, desired, ready);
            } catch (Throwable t) {
                logger.error("[applyProbed] cluster={} shard={} dbId={}",
                        desired.cluster, desired.shard, desired.dbId, t);
            }
        });
    }

    private void applyProbed(long epoch, DesiredShard desired, Map<String, Boolean> ready) {
        synchronized (refreshLock) {
            if (stopped || epoch != refreshEpoch) {
                return;
            }
            Map<String, Endpoint> accepted = new LinkedHashMap<>();
            for (Map.Entry<String, Endpoint> entry : desired.keepers.entrySet()) {
                if (Boolean.TRUE.equals(ready.get(entry.getKey()))) {
                    accepted.put(entry.getKey(), entry.getValue());
                }
            }
            if (accepted.size() < 2) {
                logger.info("[insufficientLanes] cluster={} shard={} dbId={} ready={}/{}",
                        desired.cluster, desired.shard, desired.dbId, accepted.size(), desired.keepers.size());
                safeLog(EVENT_INSUFFICIENT_LANES);
                ShardCompareTask existing = tasks.get(desired.dbId);
                if (existing != null) {
                    stopTask(existing);
                }
                return;
            }
            applyIncremental(desired, accepted);
        }
    }

    private void applyIncremental(DesiredShard desired, Map<String, Endpoint> accepted) {
        ShardCompareTask task = tasks.get(desired.dbId);
        if (task == null) {
            task = new ShardCompareTask(desired.dbId, desired.cluster, desired.shard);
            tasks.put(desired.dbId, task);
            logger.info("[addTask] cluster={} shard={} dbId={} lanes={}",
                    desired.cluster, desired.shard, desired.dbId, accepted.size());
        } else {
            task.cluster = desired.cluster;
            task.shard = desired.shard;
        }
        ShardComparator cmp = task.comparator;
        if (cmp == null) {
            try {
                cmp = startComparator(task);
            } catch (Throwable t) {
                logger.error("[startComparator] cluster={} shard={} dbId={}",
                        desired.cluster, desired.shard, desired.dbId, t);
                stopTask(task);
                return;
            }
        }
        boolean streamsChanged = false;
        List<String> stale = new ArrayList<>();
        for (String key : task.streams.keySet()) {
            if (!accepted.containsKey(key)) {
                stale.add(key);
            }
        }
        for (String key : stale) {
            closeQuietly(task.streams.remove(key), desired.cluster, desired.shard, key);
            streamsChanged = true;
        }
        for (Map.Entry<String, Endpoint> entry : accepted.entrySet()) {
            if (task.streams.containsKey(entry.getKey())) {
                continue;
            }
            try {
                CompareLane lane = streamFactory.open(desired.dbId, desired.cluster, desired.shard,
                        entry.getValue(), cmp::wake);
                task.streams.put(entry.getKey(), lane);
                streamsChanged = true;
            } catch (Throwable t) {
                logger.error("[open] cluster={} shard={} keeper={}",
                        desired.cluster, desired.shard, entry.getKey(), t);
            }
        }
        if (task.streams.size() < 2) {
            logger.info("[insufficientLanes] open leftover <2, cluster={} shard={} dbId={} lanes={}",
                    desired.cluster, desired.shard, desired.dbId, task.streams.size());
            safeLog(EVENT_INSUFFICIENT_LANES);
            stopTask(task);
            return;
        }
        if (streamsChanged) {
            cmp.replaceLanes(new ArrayList<>(task.streams.values()));
        }
    }

    private ShardComparator startComparator(ShardCompareTask task) {
        ShardComparator cmp = new ShardComparator(task.cluster, task.shard,
                Collections.emptyList(), config, reporter);
        int n = 1;
        for (ShardCompareTask existing : tasks.values()) {
            if (existing != task && existing.comparator != null) {
                n++;
            }
        }
        if (n > compareThreadWarnThreshold) {
            logger.warn("[compareThreadWarn] cluster={} shard={} threads={} threshold={}",
                    task.cluster, task.shard, n, compareThreadWarnThreshold);
            safeLog(EVENT_COMPARE_THREAD_WARN);
        }
        cmp.start();
        task.comparator = cmp;
        return cmp;
    }

    private void stopComparator(ShardCompareTask task) {
        ShardComparator cmp = task.comparator;
        task.comparator = null;
        if (cmp != null) {
            cmp.stop();
        }
    }

    private void stopTask(ShardCompareTask task) {
        stopComparator(task);
        for (Map.Entry<String, CompareLane> entry : task.streams.entrySet()) {
            closeQuietly(entry.getValue(), task.cluster, task.shard, entry.getKey());
        }
        task.streams.clear();
        tasks.remove(task.dbId);
        try {
            reporter.forgetShard(task.cluster, task.shard);
        } catch (Throwable t) {
            logger.warn("[forgetShard] cluster={} shard={}", task.cluster, task.shard, t);
        }
        try {
            streamFactory.release(task.dbId);
        } catch (Throwable t) {
            logger.error("[release] cluster={} shard={} dbId={}", task.cluster, task.shard, task.dbId, t);
        }
    }

    private void closeQuietly(CompareLane lane, String cluster, String shard, String keeper) {
        if (lane == null) {
            return;
        }
        try {
            streamFactory.close(lane);
        } catch (Throwable t) {
            logger.error("[close] cluster={} shard={} keeper={}", cluster, shard, keeper, t);
        }
    }

    private void safeLog(String name) {
        try {
            eventMonitor.logEvent(MONITOR_TYPE, name);
        } catch (Throwable t) {
            logger.warn("[logEvent] {}", name, t);
        }
    }

    private static Endpoint endpointOf(KeeperMeta keeper) {
        if (keeper == null || StringUtil.isEmpty(keeper.getIp()) || keeper.getPort() == null || keeper.getPort() <= 0) {
            return null;
        }
        return new DefaultEndPoint(keeper.getIp(), keeper.getPort());
    }

    private static String addressOf(Endpoint endpoint) {
        return endpoint.getHost() + ":" + endpoint.getPort();
    }

    @VisibleForTesting
    ShardCompareTask taskOf(long dbId) {
        synchronized (refreshLock) {
            return tasks.get(dbId);
        }
    }

    @VisibleForTesting
    public void putTask(ShardCompareTask task) {
        synchronized (refreshLock) {
            tasks.put(task.dbId, task);
        }
    }

    public static final class ShardCompareTask {

        private final long dbId;

        private volatile String cluster;

        private volatile String shard;

        private final Map<String, CompareLane> streams = new LinkedHashMap<>();

        private ShardComparator comparator;

        public ShardCompareTask(long dbId, String cluster, String shard) {
            this.dbId = dbId;
            this.cluster = cluster;
            this.shard = shard;
        }

        @VisibleForTesting
        public void bind(ShardComparator comparator, Map<String, CompareLane> streams) {
            this.comparator = comparator;
            this.streams.clear();
            if (streams != null) {
                this.streams.putAll(streams);
            }
        }

        public long getDbId() {
            return dbId;
        }

        public String getCluster() {
            return cluster;
        }

        public String getShard() {
            return shard;
        }

        public Map<String, CompareLane> getStreams() {
            return Collections.unmodifiableMap(streams);
        }

        public ShardComparator getComparator() {
            return comparator;
        }
    }

    @VisibleForTesting
    void setCompareThreadWarnThreshold(int threshold) {
        this.compareThreadWarnThreshold = threshold;
    }

    private static final class DesiredShard {

        private final long dbId;

        private final String cluster;

        private final String shard;

        private final Map<String, Endpoint> keepers = new LinkedHashMap<>();

        DesiredShard(long dbId, String cluster, String shard) {
            this.dbId = dbId;
            this.cluster = cluster;
            this.shard = shard;
        }
    }
}
