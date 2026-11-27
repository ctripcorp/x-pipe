package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Rate limits re-elections that are triggered ONLY by a keeper priority change (logical BU TFS ↔ BM switch).
 *
 * <p>Every path that changes priority ends in {@code DefaultKeeperElectorManager.handleClusterModified}: console
 * clusterModified pushes, {@code /api/dcmeta/reload} and the periodic DcMeta refresh. Throttling there bounds the
 * switch rate whichever way MetaServer learned about the change.
 *
 * <ul>
 *     <li>Pending shards are de-duplicated; a shard queued twice is re-elected once.</li>
 *     <li>{@link KeeperElectReElectService#reElect} reads survive keepers and cached priority when it runs, so a shard
 *     that waited in the queue is elected on its latest state, not on the state when it was queued.</li>
 *     <li>Rate is {@link MetaServerConfig#getKeeperPriorityReElectRatePerSecond()}, read on every tick;
 *     {@code <= 0} re-elects immediately in the caller thread (old behavior) and drains the queue.</li>
 *     <li>Keeper failover (ZK survive keeper change) does not come through here and is never delayed.</li>
 * </ul>
 */
@Component
public class KeeperPriorityReElectThrottler {

    private static final Logger logger = LoggerFactory.getLogger(KeeperPriorityReElectThrottler.class);

    public static final String METRIC_TYPE = "keeper.priority.reelect";
    static final long TICK_MILLI = 200;

    @Autowired
    private MetaServerConfig metaServerConfig;

    @Autowired
    private KeeperElectReElectService keeperElectReElectService;

    private final LinkedHashSet<Pair<Long, Long>> pending = new LinkedHashSet<>();

    /** Fractional permits carried between ticks so low rates (e.g. 1/s with 5 ticks/s) still progress. */
    private double permits = 0;

    private ScheduledExecutorService scheduled;

    @PostConstruct
    public void start() {
        scheduled = Executors.newSingleThreadScheduledExecutor(XpipeThreadFactory.create("KeeperPriorityReElect"));
        scheduled.scheduleWithFixedDelay(this::safeTick, TICK_MILLI, TICK_MILLI, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop() {
        if (scheduled != null) {
            scheduled.shutdownNow();
        }
    }

    public void submit(Long clusterDbId, Long shardDbId) {
        if (rate() <= 0) {
            reElect(clusterDbId, shardDbId);
            return;
        }
        int size;
        synchronized (pending) {
            pending.add(new Pair<>(clusterDbId, shardDbId));
            size = pending.size();
        }
        logger.info("[submit][queued]cluster_{},shard_{},pending={}", clusterDbId, shardDbId, size);
    }

    public int pendingSize() {
        synchronized (pending) {
            return pending.size();
        }
    }

    private void safeTick() {
        try {
            tick();
        } catch (Throwable th) {
            logger.error("[tick]", th);
        }
    }

    @VisibleForTesting
    void tick() {
        int rate = rate();
        List<Pair<Long, Long>> batch = new ArrayList<>();
        synchronized (pending) {
            if (pending.isEmpty()) {
                permits = 0;
                return;
            }
            int take;
            if (rate <= 0) {
                take = pending.size();
            } else {
                // never accumulate more than one second of permits while idle
                permits = Math.min(permits + rate * TICK_MILLI / 1000.0, Math.max(1, rate));
                take = (int) permits;
                permits -= take;
            }
            Iterator<Pair<Long, Long>> iterator = pending.iterator();
            while (take-- > 0 && iterator.hasNext()) {
                batch.add(iterator.next());
                iterator.remove();
            }
        }
        for (Pair<Long, Long> shard : batch) {
            reElect(shard.getKey(), shard.getValue());
        }
        if (!batch.isEmpty()) {
            logger.info("[tick]rate={}/s, reElected={}, pending={}", rate, batch.size(), pendingSize());
        }
    }

    private void reElect(Long clusterDbId, Long shardDbId) {
        try {
            keeperElectReElectService.reElect(clusterDbId, shardDbId);
            EventMonitor.DEFAULT.logEvent(METRIC_TYPE, "reElect");
        } catch (Exception e) {
            // e.g. cluster moved to another MetaServer or was deleted while queued
            logger.warn("[reElect][skip]cluster_{},shard_{}", clusterDbId, shardDbId, e);
        }
    }

    private int rate() {
        return metaServerConfig.getKeeperPriorityReElectRatePerSecond();
    }

    @VisibleForTesting
    void setMetaServerConfig(MetaServerConfig metaServerConfig) {
        this.metaServerConfig = metaServerConfig;
    }

    @VisibleForTesting
    void setKeeperElectReElectService(KeeperElectReElectService keeperElectReElectService) {
        this.keeperElectReElectService = keeperElectReElectService;
    }
}
