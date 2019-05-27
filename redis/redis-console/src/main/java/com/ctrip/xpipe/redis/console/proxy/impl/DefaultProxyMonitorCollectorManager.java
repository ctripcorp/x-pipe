package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyInfoRecorder;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.console.proxy.Ruler;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyMonitorCollectorManager implements ProxyMonitorCollectorManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyMonitorCollectorManager.class);

    private Map<ProxyModel, ProxyMonitorCollector> proxySamples = Maps.newConcurrentMap();

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private ProxyInfoRecorder proxyInfoRecorder;

    private ScheduledFuture future;

    private AtomicBoolean taskTrigger = new AtomicBoolean(false);

    @PostConstruct
    public void postConstruct() {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                if(!taskTrigger.get()) {
                    return;
                }
                update();
            }
        }, getStartTime(), getPeriodic(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void preDestroy() {
        if(future != null) {
            future.cancel(true);
        }
        proxySamples.values().forEach(result-> {
            try {
                result.stop();
            } catch (Exception e) {
                logger.error("[preDestroy]", e);
            }
        });
        proxySamples.clear();
    }

    @Override
    public ProxyMonitorCollector getOrCreate(ProxyModel proxyModel) {
        return MapUtils.getOrCreate(proxySamples, proxyModel, new ObjectFactory<ProxyMonitorCollector>() {
            @Override
            public ProxyMonitorCollector create() {
                logger.info("[create proxy monitor collector] {}", proxyModel);
                ProxyMonitorCollector result = new DefaultProxyMonitorCollector(scheduled, keyedObjectPool, proxyModel);
                result.addListener(proxyInfoRecorder);
                try {
                    result.start();
                } catch (Exception e) {
                    logger.error("[getOrCreate]", e);
                }
                return result;
            }
        });
    }

    @Override
    public List<ProxyMonitorCollector> getProxyMonitorResults() {
        return Lists.newArrayList(proxySamples.values());
    }

    @Override
    public void remove(ProxyModel proxyModel) {
        ProxyMonitorCollector result = proxySamples.remove(proxyModel);
        if(result != null) {
            try {
                result.removeListener(proxyInfoRecorder);
                result.stop();
            } catch (Exception e) {
                logger.error("[remove]", e);
            }
        }
    }


    protected void update() {
        List<ProxyModel> proxies = proxyService.getMonitorActiveProxies();

        addActiveProxies(proxies);
        removeUnusedProxies(proxies);

    }

    private void addActiveProxies(List<ProxyModel> proxies) {
        Ruler<ProxyModel> tcpPortOnlyRuler = new TcpPortOnlyProxyRuler();

        for(ProxyModel proxyModel : proxies) {
            if(proxySamples.containsKey(proxyModel))
                continue;

            if(tcpPortOnlyRuler.matches(proxyModel)) {
                getOrCreate(proxyModel);
            }
        }

    }

    private void removeUnusedProxies(List<ProxyModel> proxies) {
        Set<ProxyModel> cachedProxies = Sets.newHashSet(proxySamples.keySet());
        cachedProxies.removeAll(proxies);
        for(ProxyModel model : cachedProxies) {
            remove(model);
        }
    }

    protected int getStartTime() {
        return 2 * 1000;
    }

    protected int getPeriodic() {
        return 1000;
    }

    @Override
    public void isCrossDcLeader() {
        taskTrigger.set(true);
    }

    @Override
    public void notCrossDcLeader() {
        taskTrigger.set(false);
    }

    private final class TcpPortOnlyProxyRuler implements Ruler<ProxyModel> {

        @Override
        public boolean matches(ProxyModel proxyModel) {
            ProxyEndpoint endpoint = new DefaultProxyEndpoint(proxyModel.getUri());
            return !endpoint.isSslEnabled();
        }
    }

    /**for unit test*/
    @VisibleForTesting
    protected DefaultProxyMonitorCollectorManager setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }

    @VisibleForTesting
    protected DefaultProxyMonitorCollectorManager setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
        return this;
    }

    @VisibleForTesting
    protected DefaultProxyMonitorCollectorManager setProxyService(ProxyService proxyService) {
        this.proxyService = proxyService;
        return this;
    }
}
