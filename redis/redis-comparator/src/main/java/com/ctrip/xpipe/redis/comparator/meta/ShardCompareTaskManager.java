package com.ctrip.xpipe.redis.comparator.meta;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.comparator.balance.CompareTaskAssigner;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
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
 * {@link AbstractSpringConfigContext#SCHEDULED_EXECUTOR}{@code #execute} 回流）。
 * {@code stop()} 锁内置 {@code stopped} 并抬 {@code refreshEpoch}。
 * {@code stopped} 只在 {@code refreshLock} 内读写：{@code refresh} 进锁后、
 * {@code ++refreshEpoch} 之前看见则返回；{@code applyProbed} 见停止位或
 * epoch 不匹配则返回。拆任务时 {@link KeeperStreamFactory#release(long)}。
 * 协商异步扇出，禁止 for 循环阻塞连 Keeper。未变化的 {@code ip:port} 不重建连接。
 * 本 Phase 不起比对线程（Phase CX）。
 */
public class ShardCompareTaskManager {

    public static final String MONITOR_TYPE = "ShardCompareTaskManager";

    public static final String EVENT_INSUFFICIENT_LANES = "insufficientLanes";

    private static final Logger logger = LoggerFactory.getLogger(ShardCompareTaskManager.class);

    private final ComparatorMetaService metaService;

    private final CompareTaskAssigner assigner;

    private final PrepareWatchCache watchCache;

    private final KeeperStreamFactory streamFactory;

    private final ComparatorConfig config;

    private final ScheduledExecutorService scheduled;

    private final EventMonitor eventMonitor;

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
        this.metaService = metaService;
        this.assigner = assigner;
        this.watchCache = watchCache;
        this.streamFactory = streamFactory;
        this.config = config;
        this.scheduled = scheduled;
        this.eventMonitor = eventMonitor == null ? EventMonitor.DEFAULT : eventMonitor;
    }

    public void start() {
        if (scheduled == null) {
            throw new IllegalStateException(AbstractSpringConfigContext.SCHEDULED_EXECUTOR + " required");
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
        List<String> stale = new ArrayList<>();
        for (String key : task.streams.keySet()) {
            if (!accepted.containsKey(key)) {
                stale.add(key);
            }
        }
        for (String key : stale) {
            closeQuietly(task.streams.remove(key), desired.cluster, desired.shard, key);
        }
        for (Map.Entry<String, Endpoint> entry : accepted.entrySet()) {
            if (task.streams.containsKey(entry.getKey())) {
                continue;
            }
            try {
                CompareLane lane = streamFactory.open(desired.dbId, desired.cluster, desired.shard,
                        entry.getValue(), task.wake);
                task.streams.put(entry.getKey(), lane);
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
        }
    }

    private void stopTask(ShardCompareTask task) {
        for (Map.Entry<String, CompareLane> entry : task.streams.entrySet()) {
            closeQuietly(entry.getValue(), task.cluster, task.shard, entry.getKey());
        }
        task.streams.clear();
        tasks.remove(task.dbId);
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

    public static final class ShardCompareTask {

        private final long dbId;

        private volatile String cluster;

        private volatile String shard;

        private final Map<String, CompareLane> streams = new LinkedHashMap<>();

        private volatile Runnable wakeHook;

        private final Runnable wake = () -> {
            Runnable hook = wakeHook;
            if (hook != null) {
                hook.run();
            }
        };

        ShardCompareTask(long dbId, String cluster, String shard) {
            this.dbId = dbId;
            this.cluster = cluster;
            this.shard = shard;
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

        public void setWakeHook(Runnable wakeHook) {
            this.wakeHook = wakeHook;
        }
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
