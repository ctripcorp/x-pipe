package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ForceCloseDir for old TFS slot keeper. Gateway failure / empty params / invalid appId do not fail the chain.
 */
public class TfsForceCloseDirCommand extends AbstractCommand<Void> {

    private static final String TFS_FORCE_CLOSE_DIR_TYPE = "TfsForceCloseDir";

    private final TfsShardContext shardContext;
    private final KeeperMeta keeperMeta;
    private final DcMetaCache dcMetaCache;
    private final MetaServerConfig metaServerConfig;
    private final ScheduledExecutorService scheduled;
    private final Executor executor;
    private final TfsGateway tfsGateway;

    public TfsForceCloseDirCommand(TfsShardContext shardContext, KeeperMeta keeperMeta, DcMetaCache dcMetaCache,
                                   MetaServerConfig metaServerConfig, ScheduledExecutorService scheduled,
                                   Executor executor) {
        this(shardContext, keeperMeta, dcMetaCache, metaServerConfig, scheduled, executor,
                TfsGatewayFactory.create(metaServerConfig));
    }

    public TfsForceCloseDirCommand(TfsShardContext shardContext, KeeperMeta keeperMeta, DcMetaCache dcMetaCache,
                                   MetaServerConfig metaServerConfig, ScheduledExecutorService scheduled,
                                   Executor executor, TfsGateway tfsGateway) {
        this.shardContext = shardContext;
        this.keeperMeta = keeperMeta;
        this.dcMetaCache = dcMetaCache;
        this.metaServerConfig = metaServerConfig;
        this.scheduled = scheduled;
        this.executor = executor;
        this.tfsGateway = tfsGateway;
    }

    @Override
    public String getName() {
        return "tfs force close dir";
    }

    @Override
    protected void doExecute() throws Exception {
        KeeperContainerMeta keeperContainer = dcMetaCache.getKeeperContainer(keeperMeta);
        String fsId = keeperContainer != null ? keeperContainer.getTfsFsId() : null;
        long replId = shardContext.getShardDbId() != null ? shardContext.getShardDbId() : 0L;
        String dirPath = TfsDirPathResolver.resolve(metaServerConfig.getTfsDirPathTemplate(), keeperMeta.getPort(),
                replId);
        String podIp = keeperMeta.getIp();

        if (StringUtil.isEmpty(fsId)) {
            logError("empty fs_id", keeperMeta, dirPath, podIp, null);
            future().setSuccess(null);
            return;
        }
        if (StringUtil.isEmpty(dirPath)) {
            logError("empty dir_path", keeperMeta, dirPath, podIp, null);
            future().setSuccess(null);
            return;
        }
        if (StringUtil.isEmpty(podIp)) {
            logError("empty pod_ip", keeperMeta, dirPath, podIp, null);
            future().setSuccess(null);
            return;
        }
        if (!TfsGatewayFactory.isMockHost(metaServerConfig.getTfsGatewayHost())
                && metaServerConfig.getTfsGatewayAppId() <= 0) {
            logError("invalid appId", keeperMeta, dirPath, podIp, null);
            future().setSuccess(null);
            return;
        }

        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        ScheduledFuture<?> timeoutFuture = scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                if (completed.compareAndSet(false, true)) {
                    Thread worker = workerThread.get();
                    if (worker != null) {
                        worker.interrupt();
                    }
                    logError("timeout", keeperMeta, dirPath, podIp, null);
                    future().setSuccess(null);
                }
            }
        }, TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI, TimeUnit.MILLISECONDS);

        executor.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                workerThread.set(Thread.currentThread());
                if (completed.get()) {
                    return;
                }
                try {
                    tfsGateway.forceCloseDir(fsId, dirPath, podIp);
                    if (completed.compareAndSet(false, true)) {
                        timeoutFuture.cancel(false);
                        future().setSuccess(null);
                    }
                } catch (Exception e) {
                    if (completed.compareAndSet(false, true)) {
                        timeoutFuture.cancel(false);
                        logError("gateway fail", keeperMeta, dirPath, podIp, e);
                        future().setSuccess(null);
                    }
                }
            }
        });
    }

    private void logError(String reason, KeeperMeta keeper, String dirPath, String podIp, Throwable e) {
        if (e != null) {
            getLogger().error("[forceCloseDir][{}]cluster_{},shard_{},keeper={}, dirPath={}, podIp={}", reason,
                    shardContext.getClusterDbId(), shardContext.getShardDbId(), keeper, dirPath, podIp, e);
        } else {
            getLogger().error("[forceCloseDir][{}]cluster_{},shard_{},keeper={}, dirPath={}, podIp={}, host={}, appId={}",
                    reason, shardContext.getClusterDbId(), shardContext.getShardDbId(), keeper, dirPath, podIp,
                    metaServerConfig.getTfsGatewayHost(), metaServerConfig.getTfsGatewayAppId());
        }
        CatEventMonitor.DEFAULT.logEvent(TFS_FORCE_CLOSE_DIR_TYPE, reason);
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }
}
