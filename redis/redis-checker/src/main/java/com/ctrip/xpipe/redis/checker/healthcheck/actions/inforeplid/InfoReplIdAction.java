package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.CrossRegionKeeperSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Checks that a backup-dc redis replicates from the Keeper the meta claims: the redis'
 * {@code master_replid} must equal the upstream Keeper's {@code master_replid} or
 * {@code master_replid2}.
 *
 * <p>Threading: both INFO commands are issued from the scheduling thread, but their callbacks are
 * completed by the netty event loop (or the command-timeout thread on timeout). Every callback
 * therefore does nothing but {@link #dispatch}, which hands the work to the action's executor --
 * meta lookups deep-clone a shard and {@code findOrCreateSession} takes a shared lock, neither of
 * which belongs on an event loop shared by every channel.
 *
 * <p>{@link #inFlight} keeps at most one check per instance outstanding: the periodic task returns
 * immediately, so without it a tick would overlap the previous one. It is written from callback
 * threads and read from the scheduling thread, hence the {@link AtomicBoolean}.
 */
public class InfoReplIdAction extends AbstractHealthCheckAction<RedisHealthCheckInstance> {

    protected static final Logger logger = LoggerFactory.getLogger(InfoReplIdAction.class);

    private static final String MASTER_REPLID = "master_replid";

    private static final String MASTER_REPLID2 = "master_replid2";

    private final CrossRegionKeeperSessionManager crossRegionKeeperSessionManager;

    private final MetaCache metaCache;

    private final AtomicBoolean inFlight = new AtomicBoolean(false);

    public InfoReplIdAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                            ExecutorService executors, CrossRegionKeeperSessionManager crossRegionKeeperSessionManager,
                            MetaCache metaCache) {
        super(scheduled, instance, executors);
        this.crossRegionKeeperSessionManager = crossRegionKeeperSessionManager;
        this.metaCache = metaCache;
    }

    @Override
    protected void doTask() {
        if (!inFlight.compareAndSet(false, true)) {
            logger.debug("[doTask][previous check still running] {}", instance.getCheckInfo().getHostPort());
            return;
        }
        try {
            fetchSlaveInfo();
        } catch (Throwable th) {
            fail(th);
        }
    }

    private void fetchSlaveInfo() {
        instance.getRedisSession().info(InfoCommand.INFO_TYPE.REPLICATION,
                new Callbackable<InfoResultExtractor>() {
                    @Override
                    public void success(InfoResultExtractor slaveInfo) {
                        dispatch(() -> onSlaveInfo(slaveInfo));
                    }

                    @Override
                    public void fail(Throwable throwable) {
                        dispatch(() -> InfoReplIdAction.this.fail(throwable));
                    }
                });
    }

    private void onSlaveInfo(InfoResultExtractor slaveInfo) {
        String slaveReplId = slaveInfo.extract(MASTER_REPLID);
        String masterHost = slaveInfo.getKeyKeeperMasterHost();
        int masterPort = slaveInfo.getKeyKeeperMasterPort();
        if (slaveReplId == null || masterHost == null || masterPort <= 0) {
            logger.info("[doTask][slave info incomplete] {}", instance.getCheckInfo().getHostPort());
            finish(new InfoReplIdActionContext(instance, new IllegalStateException("slave info incomplete")));
            return;
        }

        List<KeeperMeta> keepers = metaCache.getKeeperOfDcClusterShard(
                instance.getCheckInfo().getDcId(),
                instance.getCheckInfo().getClusterId(),
                instance.getCheckInfo().getShardId());
        boolean keeperInMeta = keepers.stream()
                .anyMatch(keeper -> masterHost.equals(keeper.getIp()) && masterPort == keeper.getPort());
        if (!keeperInMeta) {
            logger.info("[doTask][keeper not in meta] {} master={}:{}",
                    instance.getCheckInfo().getHostPort(), masterHost, masterPort);
            finish(new InfoReplIdActionContext(instance,
                    new KeeperNotInMetaException(String.format("keeper not in meta, master=%s:%d", masterHost, masterPort))));
            return;
        }

        fetchKeeperInfo(slaveReplId, masterHost, masterPort);
    }

    /** Runs off the event loop already, since {@link #onSlaveInfo} is itself dispatched. */
    private void fetchKeeperInfo(String slaveReplId, String masterHost, int masterPort) {
        RedisSession upstream = crossRegionKeeperSessionManager
                .findOrCreateSession(new HostPort(masterHost, masterPort));
        upstream.info(InfoCommand.INFO_TYPE.REPLICATION, new Callbackable<InfoResultExtractor>() {
            @Override
            public void success(InfoResultExtractor keeperInfo) {
                dispatch(() -> onKeeperInfo(slaveReplId, keeperInfo));
            }

            @Override
            public void fail(Throwable throwable) {
                dispatch(() -> InfoReplIdAction.this.fail(throwable));
            }
        });
    }

    /**
     * The only thing an INFO callback does on the netty event loop. Guarantees the check always
     * reaches {@link #finish}: either the work completes and publishes, or it throws and becomes a
     * failure context. Leaving {@link #inFlight} set would silently stop this instance forever.
     */
    private void dispatch(Runnable work) {
        try {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() {
                    try {
                        work.run();
                    } catch (Throwable th) {
                        InfoReplIdAction.this.fail(th);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            // the pool uses AbortPolicy, so a shutdown (or a saturated queue) surfaces here
            logger.error("[dispatch][rejected] {}", instance.getCheckInfo().getHostPort(), e);
            inFlight.set(false);
        }
    }

    private void onKeeperInfo(String slaveReplId, InfoResultExtractor keeperInfo) {
        String keeperReplId = keeperInfo.extract(MASTER_REPLID);
        String keeperReplId2 = keeperInfo.extract(MASTER_REPLID2);
        if (keeperReplId == null) {
            logger.info("[doTask][keeper info incomplete] {}", instance.getCheckInfo().getHostPort());
            finish(new InfoReplIdActionContext(instance, new IllegalStateException("keeper info incomplete")));
            return;
        }

        boolean replIdMatch = slaveReplId.equals(keeperReplId)
                || (keeperReplId2 != null && slaveReplId.equals(keeperReplId2));
        if (!replIdMatch) {
            logger.info("[doTask][replId match={}] {} slaveReplId={}, keeperReplId={}, keeperReplId2={}",
                    replIdMatch, instance.getCheckInfo().getHostPort(), slaveReplId, keeperReplId, keeperReplId2);
        }
        finish(new InfoReplIdActionContext(instance, new Triple<>(slaveReplId, keeperReplId, keeperReplId2)));
    }

    /**
     * Sole exit of a check: releases {@link #inFlight} so the next tick may start, then publishes.
     * A result arriving after stop is dropped -- the instance may already be unregistered.
     */
    private void finish(InfoReplIdActionContext context) {
        inFlight.set(false);
        if (!getLifecycleState().isStarted()) {
            logger.debug("[finish][not started, drop] {}", instance.getCheckInfo().getHostPort());
            return;
        }
        notifyListeners(context);
    }

    private void fail(Throwable th) {
        logger.info("[doTask][fail] {}", instance.getCheckInfo().getHostPort(), th);
        finish(new InfoReplIdActionContext(instance, th));
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

}
