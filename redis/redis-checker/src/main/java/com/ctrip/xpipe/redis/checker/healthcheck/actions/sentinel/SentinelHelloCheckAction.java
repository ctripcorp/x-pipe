package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelHelloCheckAction extends AbstractLeaderAwareHealthCheckAction<RedisHealthCheckInstance> {

    private static final Logger logger = LoggerFactory.getLogger(SentinelHelloCheckAction.class);

    protected static int SENTINEL_COLLECT_INFO_INTERVAL = 5000;

    public static final String HELLO_CHANNEL = "__sentinel__:hello";

    private Set<SentinelHello> hellos = Sets.newConcurrentHashSet();

    private CheckerDbConfig checkerDbConfig;

    private Persistence persistence;

    private static final int SENTINEL_CHECK_BASE_INTERVAL = 10000;

    private Throwable subError;

    protected long lastStartTime = System.currentTimeMillis();

    public SentinelHelloCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                                    ExecutorService executors, CheckerDbConfig checkerDbConfig, Persistence persistence) {
        super(scheduled, instance, executors);
        this.checkerDbConfig = checkerDbConfig;
        this.persistence = persistence;
    }

    @Override
    protected void doTask() {
        lastStartTime = System.currentTimeMillis();
        RedisInstanceInfo info = getActionInstance().getCheckInfo();
        if (instance.getCheckInfo().isInActiveDc()) {
            logger.info("[doTask][{}-{}] in active dc, redis {}", info.getClusterId(), info.getShardId(), instance.getEndpoint());
        }

        subError = null;
        getActionInstance().getRedisSession().subscribeIfAbsent(HELLO_CHANNEL, new RedisSession.SubscribeCallback() {
            @Override
            public void message(String channel, String message) {
                executors.execute(new Runnable() {
                    @Override
                    public void run() {
                        SentinelHello hello = SentinelHello.fromString(message);
                        hellos.add(hello);
                    }
                });
            }

            @Override
            public void fail(Throwable e) {
                if (ExceptionUtils.isStackTraceUnnecessary(e)) {
                    logger.error("[sub-failed][{}] {}", getActionInstance().getCheckInfo().getHostPort(), e.getMessage());
                } else {
                    logger.error("[sub-failed][{}]", getActionInstance().getCheckInfo().getHostPort(), e);
                }
                subError = e;
            }
        });
        scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                processSentinelHellos();
            }
        }, SENTINEL_COLLECT_INFO_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @VisibleForTesting
    protected void processSentinelHellos() {
        getActionInstance().getRedisSession().closeSubscribedChannel(HELLO_CHANNEL);
        SentinelActionContext actionContext = null != subError ? new SentinelActionContext(getActionInstance(), subError) :
                new SentinelActionContext(getActionInstance(), hellos);
        notifyListeners(actionContext);
        hellos = Sets.newConcurrentHashSet();
    }

    protected boolean shouldCheck() {
        long current = System.currentTimeMillis();
        if( current - lastStartTime < getIntervalMilli()){
            logger.debug("[generatePlan][too quick {}, quit]", current - lastStartTime);
            return false;
        }

        String cluster = getActionInstance().getCheckInfo().getClusterId();
        if (!checkerDbConfig.shouldSentinelCheck(cluster)) {
            logger.warn("[doTask][BackupDc] cluster is in sentinel check whitelist, quit");

            return false;
        }

        if (persistence.isClusterOnMigration(getActionInstance().getCheckInfo().getClusterId())) {
            logger.warn("[shouldStart][{}] in migration, stop check", getActionInstance().getCheckInfo().getClusterId());
            return false;
        }

        return super.shouldCheck() && checkerDbConfig.isSentinelAutoProcess();
    }

    @Override
    protected int getBaseCheckInterval() {
        return SENTINEL_CHECK_BASE_INTERVAL;
    }

    protected int getIntervalMilli() {
        return instance.getHealthCheckConfig().getSentinelCheckIntervalMilli();
    }

}
