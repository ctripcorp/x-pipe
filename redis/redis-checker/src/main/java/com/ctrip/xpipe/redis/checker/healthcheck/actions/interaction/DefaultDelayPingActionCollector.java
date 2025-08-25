package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.ClusterHealthManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.HealthEventProcessor;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
@Component
public class DefaultDelayPingActionCollector extends AbstractDelayPingActionCollector implements DelayPingActionCollector, HealthStateService, OneWaySupport {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDelayPingActionCollector.class);

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Autowired
    private List<HealthEventProcessor> healthEventProcessors;

    @Autowired
    private ClusterHealthManager clusterHealthManager;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Autowired
    private CheckerConfig config;

    public HEALTH_STATE getState(HostPort hostPort) {
        try {
            return allHealthStatus.get(instanceManager.findRedisHealthCheckInstance(hostPort)).getState();
        } catch (Exception e) {
            return HEALTH_STATE.UNKNOWN;
        }
    }

    @PreDestroy
    public void preDestroy() {
        for(HealthStatus healthStatus : allHealthStatus.values()) {
            healthStatus.stop();
        }
    }

    @Override
    protected HealthStatus createOrGetHealthStatus(RedisHealthCheckInstance instance) {

        return MapUtils.getOrCreate(allHealthStatus, instance, new ObjectFactory<HealthStatus>() {
            @Override
            public HealthStatus create() {

                HealthStatus healthStatus;
                if (activeDcCheckerSubscribeMasterTypeInstance(instance))
                    healthStatus = new HeteroHealthStatus(instance, scheduled);
                else {
                    healthStatus = new HealthStatus(instance, scheduled);
                    healthStatus.addObserver(clusterHealthManager.createHealthStatusObserver());
                }
                healthStatus.addObserver(new Observer() {
                    @Override
                    public void update(Object args, Observable observable) {
                        onInstanceStateChange((AbstractInstanceEvent) args);
                    }
                });
                healthStatus.start();
                return healthStatus;
            }
        });
    }

    @Override
    public HEALTH_STATE getHealthState(HostPort hostPort) {
        RedisHealthCheckInstance key = allHealthStatus.keySet().stream()
                .filter(instance -> instance.getCheckInfo().getHostPort().equals(hostPort))
                .findFirst().orElse(null);

        if (null != key) return allHealthStatus.get(key).getState();
        return null;
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

    private void onInstanceStateChange(Object args) {

        logger.info("[onInstanceStateChange]{}", args);
        for (HealthEventProcessor processor : healthEventProcessors) {

            if (processor instanceof OneWaySupport) {
                executors.execute(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        logger.debug("[onInstanceStateChange] {}", processor.getClass().getSimpleName());
                        processor.onEvent((AbstractInstanceEvent) args);
                    }
                });
            }
        }
    }

    private boolean activeDcCheckerSubscribeMasterTypeInstance(RedisHealthCheckInstance instance) {
        String activeDc = instance.getCheckInfo().getActiveDc();
        if (!currentDcId.equalsIgnoreCase(activeDc)) {
            return false;
        }
        String azGroupType = instance.getCheckInfo().getAzGroupType();
        if (StringUtil.isEmpty(azGroupType)) {
            return false;
        }
        return ClusterType.lookup(azGroupType) == ClusterType.SINGLE_DC;
    }

    @VisibleForTesting
    protected void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

}