package com.ctrip.xpipe.redis.checker.healthcheck.leader;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Oct 07, 2018
 */
public abstract class AbstractLeaderAwareHealthCheckActionFactory<V extends HealthCheckInstance> implements SiteLeaderAwareHealthCheckActionFactory<V> {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource(name = SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    protected ExecutorService executors;

    @Autowired
    protected AlertManager alertManager;

    @Autowired
    private AlertPolicyManager alertPolicyManager;

    @Autowired
    private CheckerConfig checkerConfig;

    @PostConstruct
    public void registerAlertTypes() {
        for(ALERT_TYPE alertType : alertTypes()) {
            alertPolicyManager.markCheckInterval(alertType, ()->checkerConfig.getRedisConfCheckIntervalMilli());
        }
    }

    @Override
    public void destroy(SiteLeaderAwareHealthCheckAction action) {
        try {
            LifecycleHelper.stopIfPossible(action);
            action.getActionInstance().unregister(action);
        } catch (Exception e) {
            logger.error("[destroy]", e);
        }
    }

    protected abstract List<ALERT_TYPE> alertTypes();

    protected abstract List<V> getAllInstances();

    @Override
    public void isleader() {
        new SafeLoop<V>(executors, getAllInstances()) {
            @Override
            public void doRun0(V instance) {
                ClusterType clusterType = instance.getCheckInfo().getClusterType();
                if ((clusterType.equals(ClusterType.BI_DIRECTION) && AbstractLeaderAwareHealthCheckActionFactory.this instanceof BiDirectionSupport)
                        || (clusterType.equals(ClusterType.ONE_WAY) && AbstractLeaderAwareHealthCheckActionFactory.this instanceof OneWaySupport
                        || clusterType.equals(ClusterType.LOCAL_DC) && AbstractLeaderAwareHealthCheckActionFactory.this instanceof LocalDcSupport
                        || clusterType.equals(ClusterType.SINGLE_DC) && AbstractLeaderAwareHealthCheckActionFactory.this instanceof SingleDcSupport)) {
                    registerTo(instance);
                }
            }
        }.run();
    }

    @Override
    public void notLeader() {
        new SafeLoop<V>(executors, getAllInstances()) {
            @Override
            public void doRun0(V instance) {
                removeFrom(instance);
            }
        }.run();
    }

    private void registerTo(V instance) {
        SiteLeaderAwareHealthCheckAction action = create(instance);
        instance.register(action);
        try {
            LifecycleHelper.initializeIfPossible(action);
            LifecycleHelper.startIfPossible(action);
        } catch (Exception e) {
            instance.unregister(action);
            logger.error("[registerTo][{}]", instance, e);
        }

    }

    private void removeFrom(V instance) {
        HealthCheckAction target = null;
        for(Object action : instance.getHealthCheckActions()) {
            if(action.getClass().isAssignableFrom(support())) {
                target = (HealthCheckAction) action;
                break;
            }
        }
        if(target != null) {
            instance.unregister(target);
            destroy((SiteLeaderAwareHealthCheckAction) target);
        }
    }
}
