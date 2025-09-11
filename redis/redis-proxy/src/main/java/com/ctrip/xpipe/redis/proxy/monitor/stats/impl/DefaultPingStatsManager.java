package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStatsManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.proxy.spring.Production.GLOBAL_ENDPOINT_MANAGER;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
@Component
public class DefaultPingStatsManager implements PingStatsManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPingStatsManager.class);

    protected static final int CHECK_INTERVAL = 1000;

    private static final int INSTANCE_NUM_FOR_ONE_ENDPOINT = 3;

    @Autowired
    private ResourceManager resourceManager;

    @Resource(name = GLOBAL_ENDPOINT_MANAGER)
    private ProxyEndpointManager endpointManager;

    private List<PingStats> allPingStats = Lists.newCopyOnWriteArrayList();

    private Set<ProxyEndpoint> allEndpoints = Sets.newHashSet();

    private ScheduledFuture future;

    public DefaultPingStatsManager() {
    }

    public DefaultPingStatsManager(ResourceManager resourceManager, ProxyEndpointManager endpointManager) {
        this.resourceManager = resourceManager;
        this.endpointManager = endpointManager;
    }

    @Override
    public List<PingStats> getAllPingStats() {
        return allPingStats;
    }

    @Override
    public PingStats create(ProxyEndpoint endpoint) {
        PingStats pingStats = new DefaultPingStats(resourceManager.getGlobalSharedScheduled(),
                endpoint, resourceManager.getKeyedObjectPool());
        allPingStats.add(pingStats);
        allEndpoints.add(endpoint);
        try {
            pingStats.start();
        } catch (Exception e) {
            logger.error("[create]", e);
        }
        return pingStats;
    }

    @PostConstruct
    public void postConstruct() {
        ScheduledExecutorService scheduled = resourceManager.getGlobalSharedScheduled();
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                if(resourceManager.getProxyConfig().startMonitor()) {
                    createOrRemove();
                }
            }
        }, CHECK_INTERVAL, CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void preDestroy() {
        if(future != null) {
            future.cancel(true);
        }
        for(PingStats pingStats : allPingStats) {
            try {
                pingStats.stop();
            } catch (Exception e) {
                logger.error("[preDestroy]", e);
            }
        }
    }

    private void createOrRemove() {
        List<ProxyEndpoint> endpoints = endpointManager.getAllProxyEndpoints();
        Set<ProxyEndpoint> news = newEndpoints(endpoints);
        Set<ProxyEndpoint> removed = removedEndpoints(endpoints);

        for(ProxyEndpoint endpoint : removed) {
            try {
                remove(endpoint);
            } catch (Exception e) {
                logger.error("[remove]", e);
            }
        }
        allEndpoints.removeAll(removed);

        for(ProxyEndpoint endpoint : news) {
            for(int i = 0; i < INSTANCE_NUM_FOR_ONE_ENDPOINT; i++) {
                create(endpoint);
            }
        }
    }

    private void remove(ProxyEndpoint endpoint) throws Exception {
        for(PingStats pingStats : allPingStats) {
            if(pingStats.getEndpoint().equals(endpoint)) {
                pingStats.stop();
                allPingStats.remove(pingStats);
            }
        }
    }

    private Set<ProxyEndpoint> newEndpoints(List<ProxyEndpoint> endpoints) {
        Set<ProxyEndpoint> result = Sets.newHashSet();
        for(ProxyEndpoint endpoint : endpoints) {
            if(endpoint.isProxyProtocolSupported() && !allEndpoints.contains(endpoint)) {
                result.add(endpoint);
            }
        }
        return result;
    }

    private Set<ProxyEndpoint> removedEndpoints(List<ProxyEndpoint> endpoints) {
        Set<ProxyEndpoint> result = Sets.newHashSet();
        for(ProxyEndpoint endpoint : allEndpoints) {
            if(!endpoints.contains(endpoint)) {
                result.add(endpoint);
            }
        }
        return result;
    }

    @VisibleForTesting
    protected ResourceManager getResourceManager() {
        return resourceManager;
    }

    @VisibleForTesting
    protected ProxyEndpointManager getEndpointManager() {
        return endpointManager;
    }

    public DefaultPingStatsManager setResourceManager(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
        return this;
    }

    public DefaultPingStatsManager setEndpointManager(ProxyEndpointManager endpointManager) {
        this.endpointManager = endpointManager;
        return this;
    }
}
