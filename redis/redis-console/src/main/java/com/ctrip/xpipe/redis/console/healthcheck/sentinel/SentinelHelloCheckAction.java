package com.ctrip.xpipe.redis.console.healthcheck.sentinel;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.AbstractDCLAHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
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
public class SentinelHelloCheckAction extends AbstractDCLAHealthCheckAction {

    private static final Logger logger = LoggerFactory.getLogger(SentinelHelloCheckAction.class);

    protected static int SENTINEL_COLLECT_INFO_INTERVAL = 5000;

    public static final String HELLO_CHANNEL = "__sentinel__:hello";

    private Set<SentinelHello> hellos = Sets.newHashSet();

    private MetaCache metaCache;

    private ConsoleDbConfig consoleDbConfig;

    public SentinelHelloCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                                    ExecutorService executors, MetaCache metaCache, ConsoleDbConfig consoleDbConfig) {
        super(scheduled, instance, executors);
        this.metaCache = metaCache;
        this.consoleDbConfig = consoleDbConfig;
    }

    @Override
    protected void doScheduledTask() {
        if(!shouldStart()) {
            return;
        }
        getActionInstance().getRedisSession().subscribeIfAbsent(HELLO_CHANNEL, new RedisSession.SubscribeCallback() {
            @Override
            public void message(String channel, String message) {
                SentinelHello hello = SentinelHello.fromString(message);
                hellos.add(hello);
            }

            @Override
            public void fail(Throwable e) {
                logger.error("[sub-failed]", e);
            }
        });
        scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                processSentinelHellos();
            }
        }, SENTINEL_COLLECT_INFO_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    protected void processSentinelHellos() {
        getActionInstance().getRedisSession().closeSubscribedChannel(HELLO_CHANNEL);
        notifyListeners(new SentinelActionContext(getActionInstance(), hellos));
        hellos = Sets.newHashSet();
    }

    private boolean shouldStart() {
        boolean inBackupDc = metaCache.inBackupDc(getActionInstance().getRedisInstanceInfo().getHostPort());
        if(!inBackupDc) {
            logger.debug("[doScheduledTask][BackupDc] quit");
            return false;
        }
        return consoleDbConfig.isSentinelAutoProcess();
    }
}
