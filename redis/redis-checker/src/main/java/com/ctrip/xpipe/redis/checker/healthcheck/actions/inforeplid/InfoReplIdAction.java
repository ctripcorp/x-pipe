package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.CommandChainException;
import com.ctrip.xpipe.command.ExecutorSequenceCommandChain;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.CrossRegionKeeperSessionManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Checks that a backup-dc redis replicates from the Keeper the meta claims: the redis'
 * {@code master_replid} must equal the upstream Keeper's {@code master_replid} or
 * {@code master_replid2}.
 *
 * <p>The check is a {@link SequenceCommandChain} of two INFO commands -- the redis' own, then the
 * Keeper's. {@link ExecutorSequenceCommandChain} keeps every stage on this action's executor, so
 * neither the parsing nor the meta lookup lands on the netty event loop that completed the previous
 * stage.
 *
 * <p>Ticks never wait for each other: each one starts a check and stamps it with
 * {@link #latestCheck}, and a result is discarded only when one from a newer check has already been
 * published -- i.e. when it came back late.
 *
 * <p>There is deliberately no in-flight guard. Such a guard is a latch that a check failing to
 * complete would leave closed, muting this instance with nothing able to release it -- the stage
 * between the two INFO commands is covered by no command timeout, so that is reachable. Here the
 * worst case of a check that never completes is discarded work, never silence.
 */
public class InfoReplIdAction extends AbstractHealthCheckAction<RedisHealthCheckInstance> {

    protected static final Logger logger = LoggerFactory.getLogger(InfoReplIdAction.class);

    private final CrossRegionKeeperSessionManager crossRegionKeeperSessionManager;

    private final MetaCache metaCache;

    /** Stamp handed to each started check. */
    private final AtomicLong latestCheck = new AtomicLong();

    /** Stamp of the newest check whose result has been published. */
    private final AtomicLong publishedCheck = new AtomicLong();

    public InfoReplIdAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                            ExecutorService executors, CrossRegionKeeperSessionManager crossRegionKeeperSessionManager,
                            MetaCache metaCache) {
        super(scheduled, instance, executors);
        this.crossRegionKeeperSessionManager = crossRegionKeeperSessionManager;
        this.metaCache = metaCache;
    }

    @Override
    protected void doTask() {
        long checkId = latestCheck.incrementAndGet();

        SessionInfoCommand slaveInfo = new SessionInfoCommand(instance.getRedisSession());
        KeeperReplIdCommand keeperReplId = new KeeperReplIdCommand(instance, slaveInfo.future(),
                crossRegionKeeperSessionManager, metaCache, executors);

        SequenceCommandChain chain = new ExecutorSequenceCommandChain(executors);
        chain.add(slaveInfo);
        chain.add(keeperReplId);
        chain.future().addListener(chainFuture -> publish(checkId, chainFuture, keeperReplId.future()));
        chain.execute(executors);
    }

    /**
     * Publishes a finished check, unless it has been superseded.
     *
     * <p>Package-private so a test can drive it with a chain that failed before its second stage ran;
     * that is the one path on which {@code resultFuture} has not completed by itself.
     */
    void publish(long checkId, CommandFuture<?> chainFuture,
                 CommandFuture<Triple<String, String, String>> resultFuture) {
        Throwable cause = chainFuture.isSuccess() ? null : domainCause(chainFuture.cause());
        if (null != cause) {
            // A chain stops at its failing stage, so the keeper stage may not have run at all and its
            // future would stay incomplete for good. Close it before anything else: the command
            // object outlives this notification, and a future handed to a listener must always reach
            // a terminal state. (When the keeper stage did run and failed, it is already done.)
            if (!resultFuture.isDone()) {
                resultFuture.setFailure(
                        new IllegalStateException("keeper stage skipped, a previous stage failed", cause));
            }
            // stage failures are ordinary here (a command timeout, an INFO without a master), and
            // the stages already logged the domain ones; this records the check as a whole
            logger.info("[publish][fail] {}", instance.getCheckInfo().getHostPort(), cause);
        }

        if (!getLifecycleState().isStarted()) {
            logger.debug("[publish][not started, drop] {}", instance.getCheckInfo().getHostPort());
            return;
        }

        if (!claimPublish(checkId)) {
            logger.info("[publish][late, drop] check {} arrived after newer result {} {}",
                    checkId, publishedCheck.get(), instance.getCheckInfo().getHostPort());
            return;
        }

        if (null == cause) {
            notifyListeners(new InfoReplIdActionContext(instance, resultFuture.getNow()));
        } else {
            notifyListeners(new InfoReplIdActionContext(instance, cause));
        }
    }

    /**
     * Claims the right to publish this check's result, dropping it only when one from a newer check
     * has already been published -- i.e. when it came back late.
     *
     * <p>The comparison is against the last <em>published</em> stamp, not the last <em>started</em>
     * one. Were it the latter, a check that consistently outlives a tick interval would always find a
     * newer check already started and every result would be dropped: slow would mean silent, which is
     * the very outcome the stamp exists to avoid.
     */
    private boolean claimPublish(long checkId) {
        while (true) {
            long published = publishedCheck.get();
            if (checkId <= published) {
                return false;
            }
            if (publishedCheck.compareAndSet(published, checkId)) {
                return true;
            }
        }
    }

    /**
     * The chain reports a stage failure wrapped in {@link CommandChainException}, but listeners
     * branch on the concrete type (a {@link KeeperNotInMetaException} alerts, anything else clears
     * the repl-ids), so hand them the original cause.
     */
    private Throwable domainCause(Throwable cause) {
        Throwable unwrapped = cause;
        while (unwrapped instanceof CommandChainException && null != unwrapped.getCause()) {
            unwrapped = unwrapped.getCause();
        }
        return unwrapped;
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @VisibleForTesting
    long latestCheckStamp() {
        return latestCheck.get();
    }

}
