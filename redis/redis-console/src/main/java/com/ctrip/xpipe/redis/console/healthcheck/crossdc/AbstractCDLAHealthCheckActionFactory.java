package com.ctrip.xpipe.redis.console.healthcheck.crossdc;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 07, 2018
 */
public abstract class AbstractCDLAHealthCheckActionFactory implements CrossDcLeaderAwareHealthCheckActionFactory {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    protected ExecutorService executors;

    @Autowired
    protected AlertManager alertManager;

    @Autowired
    private AlertPolicyManager alertPolicyManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private HealthCheckInstanceManager healthCheckInstanceManager;

    @PostConstruct
    public void registerAlertTypes() {
        for(ALERT_TYPE alertType : alertTypes()) {
            alertPolicyManager.markCheckInterval(alertType, ()->consoleConfig.getRedisConfCheckIntervalMilli());
        }
    }

    @Override
    public void destroy(CrossDcLeaderAwareHealthCheckAction action) {
        try {
            LifecycleHelper.stopIfPossible(action);
            action.getActionInstance().unregister(action);
        } catch (Exception e) {
            logger.error("[destroy]", e);
        }
    }

    protected abstract List<ALERT_TYPE> alertTypes();

    @Override
    public void isCrossDcLeader() {
        new SafeLoop<RedisHealthCheckInstance>(executors, healthCheckInstanceManager.getAllRedisInstance()) {
            @Override
            public void doRun0(RedisHealthCheckInstance instance) {
                registerTo(instance);
            }
        }.run();
    }

    @Override
    public void notCrossDcLeader() {
        new SafeLoop<RedisHealthCheckInstance>(executors, healthCheckInstanceManager.getAllRedisInstance()) {
            @Override
            public void doRun0(RedisHealthCheckInstance instance) {
                removeFrom(instance);
            }
        }.run();
    }

    private void registerTo(RedisHealthCheckInstance instance) {
        CrossDcLeaderAwareHealthCheckAction action = create(instance);
        instance.register(action);
        try {
            LifecycleHelper.initializeIfPossible(action);
            LifecycleHelper.startIfPossible(action);
        } catch (Exception e) {
            instance.unregister(action);
            logger.error("[registerTo][{}]", instance, e);
        }

    }

    private void removeFrom(RedisHealthCheckInstance instance) {
        HealthCheckAction target = null;
        for(HealthCheckAction action : instance.getHealthCheckActions()) {
            if(action.getClass().isAssignableFrom(support())) {
                target = action;
                break;
            }
        }
        if(target != null) {
            instance.unregister(target);
            destroy((CrossDcLeaderAwareHealthCheckAction) target);
        }
    }
}
