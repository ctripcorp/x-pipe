package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.HealthEventProcessor;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe.PsubPingActionCollector;
import com.ctrip.xpipe.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class DefaultPsubPingActionCollector extends AbstractPsubPingActionCollector implements PsubPingActionCollector, HealthStateService, OneWaySupport {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPsubPingActionCollector.class);

    @Autowired
    private List<HealthEventProcessor> healthEventProcessors;

    @Autowired
    private CheckerConfig config;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Override
    public HEALTH_STATE getHealthState(HostPort hostPort) {
        RedisHealthCheckInstance key = allHealthStatus.keySet().stream()
                .filter(instance -> instance.getCheckInfo().getHostPort().equals(hostPort))
                .findFirst().orElse(null);

        if (null != key) return allHealthStatus.get(key).getState();
        return HEALTH_STATE.UNKNOWN;
    }

    @Override
    public HealthStatusDesc getHealthStatusDesc(HostPort hostPort) {
        HealthStatus status = getHealthStatus(hostPort);
        if (null != status) {
            long timeoutMill = config.getMarkdownInstanceMaxDelayMilli() + config.getCheckerMetaRefreshIntervalMilli();
            return new HealthStatusDesc(hostPort, status, status.getLastMarkHandled(timeoutMill));
        } else {
            return new HealthStatusDesc(hostPort, HEALTH_STATE.UNKNOWN);
        }
    }

    @Override
    public void updateLastMarkHandled(HostPort hostPort, boolean lastMark) {
        HealthStatus status = getHealthStatus(hostPort);
        if (null != status) {
            status.updateLastMarkHandled(lastMark);
        }
    }

    @Override
    public Map<HostPort, HEALTH_STATE> getAllCachedState() {
        Map<HostPort, HEALTH_STATE> cachedHealthStatus = new HashMap<>();
        allHealthStatus.forEach(((instance, healthStatus) -> {
            RedisInstanceInfo info = instance.getCheckInfo();
            cachedHealthStatus.put(info.getHostPort(), healthStatus.getState());
        }));

        return cachedHealthStatus;
    }

    public Map<HostPort, HealthStatusDesc> getAllHealthStatus() {
        Map<HostPort, HealthStatusDesc> cachedHealthStatus = new HashMap<>();
        allHealthStatus.forEach(((instance, healthStatus) -> {
            HostPort hostPort = instance.getCheckInfo().getHostPort();
            cachedHealthStatus.put(hostPort, new HealthStatusDesc(hostPort, healthStatus));
        }));

        return cachedHealthStatus;
    }

    @Override
    public void updateHealthState(Map<HostPort, HEALTH_STATE> redisStates) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected HealthStatus createOrGetHealthStatus(RedisHealthCheckInstance instance) {
        return MapUtils.getOrCreate(allHealthStatus, instance, new ObjectFactory<HealthStatus>() {
            @Override
            public HealthStatus create() {

                HealthStatus healthStatus = new CrossRegionRedisHealthStatus(instance, scheduled);

                healthStatus.addObserver(new Observer() {
                    @Override
                    public void update(Object args, Observable observable) {
                        onInstanceStateChange(args);
                    }
                });
                healthStatus.start();
                return healthStatus;
            }
        });
    }

    private void onInstanceStateChange(Object args) {

        logger.info("[onInstanceStateChange]{}", args);
        for (HealthEventProcessor processor : healthEventProcessors) {

            if (processor instanceof OneWaySupport) {
                executors.execute(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        processor.onEvent((AbstractInstanceEvent) args);
                    }
                });
            }
        }
    }

}
