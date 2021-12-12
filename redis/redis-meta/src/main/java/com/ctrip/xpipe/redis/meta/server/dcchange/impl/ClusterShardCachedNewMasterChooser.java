package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * @author lishanglin
 * date 2021/6/3
 */
public class ClusterShardCachedNewMasterChooser implements NewMasterChooser {

    private NewMasterChooser innerChooser;

    private AtomicLong expiredAt;

    private AtomicLong updateAt;

    private RedisMeta cachedNewMaster;

    private LongSupplier timeoutMilliSupplier;

    private ExpireListener expireListener;

    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> scheduledFuture;

    private static Map<Pair<Long, Long>, ClusterShardCachedNewMasterChooser> instances;

    public static ClusterShardCachedNewMasterChooser wrapChooser(Long clusterDbId, Long shardDbId, NewMasterChooser chooser,
                                               LongSupplier timeoutMilliSupplier, ScheduledExecutorService scheduled) {
        if (null == instances) {
            synchronized (ClusterShardCachedNewMasterChooser.class) {
                if (null == instances) instances = Maps.newConcurrentMap();
            }
        }

        Pair<Long, Long> key = new Pair<>(clusterDbId, shardDbId);
        return MapUtils.getOrCreate(instances, key, () ->
                new ClusterShardCachedNewMasterChooser(chooser, timeoutMilliSupplier, scheduled, (expiredChooser) -> {
                    // release expired cached-chooser even if chooser is hold by others and may exist multi cached-chooser in global
                    // because chooser is bind to dc-change-action one by one and released soon
                    instances.remove(key, expiredChooser);
                }));
    }

    private ClusterShardCachedNewMasterChooser(NewMasterChooser chooser, LongSupplier timeoutMilliSupplier,
                                               ScheduledExecutorService scheduled, ExpireListener expireListener) {
        this.innerChooser = chooser;
        this.timeoutMilliSupplier = timeoutMilliSupplier;
        this.scheduled = scheduled;
        this.expireListener = expireListener;
        this.expiredAt = new AtomicLong();
        this.updateAt = new AtomicLong();
        this.cachedNewMaster = null;
    }

    @Override
    public synchronized RedisMeta choose(List<RedisMeta> redises) {
        if (!isExpiredOrSystemTimeRollback()
                && null != cachedNewMaster && redises.contains(cachedNewMaster)) {
            return cachedNewMaster;
        }

        RedisMeta newMaster = innerChooser.choose(redises);
        long current = System.currentTimeMillis();
        long timeoutMilli = timeoutMilliSupplier.getAsLong();

        cachedNewMaster = newMaster;
        expiredAt.set(current + timeoutMilli);
        updateAt.set(current);

        if (null != scheduledFuture && !scheduledFuture.isDone()) {
            scheduledFuture.cancel(false);
        }

        scheduledFuture = scheduled.schedule(() -> {
            if (isExpiredOrSystemTimeRollback()) {
                expireListener.onExpire(this);
            }
        }, timeoutMilli, TimeUnit.MILLISECONDS);

        return newMaster;
    }

    private boolean isExpiredOrSystemTimeRollback() {
        long current = System.currentTimeMillis();
        return current >= expiredAt.get() || current < updateAt.get();
    }

    @FunctionalInterface
    private interface ExpireListener {

        void onExpire(ClusterShardCachedNewMasterChooser expiredChooser);

    }

    @VisibleForTesting
    protected void setUpdatedAt(long updatedAt) {
        this.updateAt.set(updatedAt);
    }

    @VisibleForTesting
    protected static void clear() {
        synchronized (ClusterShardCachedNewMasterChooser.class) {
            instances = null;
        }
    }

}
