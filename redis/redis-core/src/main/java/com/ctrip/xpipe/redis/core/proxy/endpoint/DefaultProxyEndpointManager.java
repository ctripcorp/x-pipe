package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.THREAD_POOL_TIME_OUT;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultProxyEndpointManager implements ProxyEndpointManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyEndpointManager.class);

    private Set<ProxyEndpoint> allEndpoints = Sets.newConcurrentHashSet();

    private Set<ProxyEndpoint> availableEndpoints = Sets.newConcurrentHashSet();

    private ProxyEndpointHealthChecker healthChecker;

    private Future future;

    private ScheduledExecutorService scheduled;

    private IntSupplier healthCheckInterval;

    public DefaultProxyEndpointManager(IntSupplier checkInterval) {
        this.healthCheckInterval = checkInterval;
        this.scheduled = MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create("ProxyEndpointManager")),
                THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
        this.healthChecker = new DefaultProxyEndpointHealthChecker(scheduled);
        start();
    }

    @Override
    public List<ProxyEndpoint> getAvailableProxyEndpoints() {
        return Lists.newArrayList(availableEndpoints);
    }

    @Override
    public List<ProxyEndpoint> getAllProxyEndpoints() {
        return Lists.newArrayList(allEndpoints);
    }

    @Override
    public void storeProxyEndpoints(List<ProxyEndpoint> endpoints) {
        List<ProxyEndpoint> newArrival = Lists.newArrayList(endpoints);
        newArrival.removeAll(allEndpoints);
        allEndpoints.addAll(newArrival);
        availableEndpoints.addAll(newArrival);
    }

    @Override
    public void start() {
        future = scheduled.scheduleWithFixedDelay(new HealthCheckTask(),
                healthCheckInterval.getAsInt(), healthCheckInterval.getAsInt(), TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        if(future != null) {
            future.cancel(true);
        }
    }

    @VisibleForTesting
    public DefaultProxyEndpointManager setHealthChecker(ProxyEndpointHealthChecker healthChecker) {
        this.healthChecker = healthChecker;
        return this;
    }

    class HealthCheckTask extends AbstractExceptionLogTask {

        @Override
        protected void doRun() throws Exception {
            for(ProxyEndpoint endpoint : allEndpoints) {
                if(healthChecker.checkConnectivity(endpoint)) {
                    availableEndpoints.add(endpoint);
                } else {
                    availableEndpoints.remove(endpoint);
                    healthChecker.resetIfNeed(endpoint);
                }
            }
            DefaultProxyEndpointManager.logger.debug("[HealthCheckTask] all endpoints: {}, available endpoints: {}",
                    getAllProxyEndpoints(), getAvailableProxyEndpoints());
        }
    }
}
