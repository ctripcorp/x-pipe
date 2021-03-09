package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.healthcheck.CheckInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/14
 */
public abstract class AbstractHealthCheckInstance<T extends CheckInfo> extends AbstractLifecycle implements HealthCheckInstance<T> {

    private List<HealthCheckAction> actions = Lists.newCopyOnWriteArrayList();

    protected T instanceInfo;

    private HealthCheckConfig healthCheckConfig;

    public AbstractHealthCheckInstance setInstanceInfo(T instanceInfo) {
        this.instanceInfo = instanceInfo;
        return this;
    }

    public AbstractHealthCheckInstance setHealthCheckConfig(HealthCheckConfig healthCheckConfig) {
        this.healthCheckConfig = healthCheckConfig;
        return this;
    }

    @Override
    public T getCheckInfo() {
        return instanceInfo;
    }

    @Override
    public HealthCheckConfig getHealthCheckConfig() {
        return healthCheckConfig;
    }

    @Override
    public void register(HealthCheckAction action) {
        actions.add(action);
    }

    @Override
    public void unregister(HealthCheckAction action) {
        actions.remove(action);
    }

    @Override
    public List<HealthCheckAction> getHealthCheckActions() {
        return actions;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        for(HealthCheckAction action : actions) {
            LifecycleHelper.initializeIfPossible(action);
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        for(HealthCheckAction action : actions) {
            LifecycleHelper.startIfPossible(action);
        }
    }

    @Override
    protected void doStop() throws Exception {
        for(HealthCheckAction action : actions) {
            try {
                LifecycleHelper.stopIfPossible(action);
            } catch (Exception e) {
                logger.error("[stop] {}", this.toString(), e);
            }
        }
        actions.clear();
        super.doStop();
    }

}
