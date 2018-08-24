package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
@Component
public class DefaultPingHealthEventProcessor implements PingHealthEventProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPingHealthEventProcessor.class);

    public static final int PING_FAIL_INTERVAL_MILLI = Integer.parseInt(System.getProperty("console.ping.down.interval", "200"));

    private OuterClientService outerClientService = OuterClientService.DEFAULT;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private ConsoleServiceManager consoleServiceManager;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private PingDownStrategy pingDownStrategy;

    private final Object lock = new Object();

    private Set<RedisHealthCheckInstance> pingDownInstances = Sets.newConcurrentHashSet();

    private AtomicBoolean pingDownCheckTriggered = new AtomicBoolean(false);

    @Override
    public void markDown(RedisHealthCheckInstance instance) {
        if(!inBackupDc(instance)) {
            return;
        }
        quorumMarkDown(instance);

    }

    @Override
    public void markUp(RedisHealthCheckInstance instance) {
        boolean isDelayHealthy = instance.getHealthCheckContext().getDelayContext().isHealthy();
        if(!isDelayHealthy) {
            logger.info("[doMarkUp] ping healthy, delay unhealthy, do not mark up: {}", instance.getRedisInstanceInfo());
            return;
        }
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        try {
            outerClientService.markInstanceUp(info.getClusterShardHostport());
        } catch (OuterClientException e) {
            logger.error("[doMarkDown]", e);
        }
    }

    private boolean inBackupDc(RedisHealthCheckInstance instance) {
        return metaCache.inBackupDc(instance.getRedisInstanceInfo().getHostPort());
    }

    private void quorumMarkDown(RedisHealthCheckInstance instance) {
        List<Boolean> pingStatus = consoleServiceManager.allPingStatus(instance.getEndpoint().getHost(),
                instance.getEndpoint().getPort());

        boolean quorum = consoleServiceManager.quorumSatisfy(pingStatus, (result)->!result);
        if(!quorum) {
            RedisInstanceInfo info = instance.getRedisInstanceInfo();
            logger.info("[quorumMarkInstanceDown][quorum fail]{}, {}", info.getClusterShardHostport(), quorum);
            alertManager.alert(
                    info.getClusterId(),
                    info.getShardId(),
                    info.getHostPort(),
                    ALERT_TYPE.QUORUM_DOWN_FAIL,
                    info.getHostPort().toString()
            );
        } else {
            boolean success = false;
            synchronized (lock) {
                success = pingDownInstances.add(instance);
            }
            if(success) {
                if (pingDownCheckTriggered.compareAndSet(false, true)) {
                    scheduled.schedule(new AbstractExceptionLogTask() {
                        @Override
                        protected void doRun() throws Exception {
                            checkPingDownEvent();
                        }
                    }, PING_FAIL_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void checkPingDownEvent() {
        Set<RedisHealthCheckInstance> set;
        synchronized (lock) {
            set = pingDownInstances;
            pingDownInstances = Sets.newConcurrentHashSet();
            pingDownCheckTriggered.compareAndSet(true, false);
        }
        PingDownStrategy.PingDownResult result = pingDownStrategy.getPingDownResult(set);
        dealPingDownEvents(result);
    }

    private void dealPingDownEvents(PingDownStrategy.PingDownResult result) {
        for(RedisHealthCheckInstance instance : result.getPingDownInstances()) {
            try {
                outerClientService.markInstanceDown(instance.getRedisInstanceInfo().getClusterShardHostport());
            } catch (OuterClientException e) {
                logger.error("[dealPingDownEvents]", e);
            }
        }

        for(RedisHealthCheckInstance instance : result.getIgnoredPingDownInstances()) {
            RedisInstanceInfo info = instance.getRedisInstanceInfo();
            alertManager.alert(info.getClusterId(), info.getShardId(), info.getHostPort(),
                    ALERT_TYPE.PING_DOWN_TOO_MUCH, info.getDcId());
            logger.info("[dealPingDownEvents]ping down too much: {}", info);
        }
    }

}
