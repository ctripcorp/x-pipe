package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.console.proxy.Ruler;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyMonitorCollectorManager implements ProxyMonitorCollectorManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyMonitorCollectorManager.class);

    private Map<ProxyModel, ProxyMonitorCollector> proxySamples = Maps.newConcurrentMap();

    private List<Listener> listeners = Lists.newCopyOnWriteArrayList();

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private RouteService routeService;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    private ExecutorService executors;

    private ScheduledFuture future;

    private final AtomicInteger updateCounter = new AtomicInteger(0);

    @Override
    public ProxyMonitorCollector getOrCreate(ProxyModel proxyModel) {
        return MapUtils.getOrCreate(proxySamples, proxyModel, new ObjectFactory<ProxyMonitorCollector>() {
            @Override
            public ProxyMonitorCollector create() {
                ProxyMonitorCollector result = new DefaultProxyMonitorCollector(scheduled, keyedObjectPool, proxyModel);
                result.addListener(DefaultProxyMonitorCollectorManager.this);
                try {
                    result.start();
                } catch (Exception e) {
                    logger.error("[getOrCreate]", e);
                }
                notifyListeners(proxyModel, ProxyMonitorCollectType.CREATE);
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
                result.removeListener(this);
                result.stop();
            } catch (Exception e) {
                logger.error("[remove]", e);
            }
        }
        notifyListeners(proxyModel, ProxyMonitorCollectType.DELETE);
    }

    @Override
    public void onChange(ProxyMonitorCollector collector) {
        int count = updateCounter.incrementAndGet();
        if(count == proxySamples.size()) {
            synchronized (updateCounter) {
                updateCounter.set(0);
            }
            update();
        }
    }

    @Override
    public synchronized void register(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public synchronized void stopNotify(Listener listener) {
        listeners.remove(listener);
    }

    protected synchronized void notifyListeners(ProxyMonitorCollectType type) {
        for(Listener listener : listeners) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    listener.onGlobalEvent(type);
                }
            });
        }
    }

    protected synchronized void notifyListeners(ProxyModel proxyModel, ProxyMonitorCollectType type) {
        for(Listener listener : listeners) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    listener.onLocalEvent(type, proxyModel);
                }
            });
        }
    }

    @PostConstruct
    public void postConstruct() {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
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

    protected void update() {
        List<ProxyModel> proxies = proxyService.getActiveProxies();
        AtomicInteger changes = new AtomicInteger(0);

        addActiveProxies(proxies, changes);
        removeUsedProxies(proxies, changes);

        if(changes.get() > 0) {
            notifyListeners(ProxyMonitorCollectType.UPDATE);
        }
    }

    private void addActiveProxies(List<ProxyModel> proxies, AtomicInteger changes) {
        Ruler<ProxyModel> inUseRuler = new ProxyInUseRuler();
        Ruler<ProxyModel> tcpPortOnlyRuler = new TcpPortOnlyProxyRuler();

        for(ProxyModel proxyModel : proxies) {
            if(proxySamples.containsKey(proxyModel))
                continue;

            if(inUseRuler.matches(proxyModel) && tcpPortOnlyRuler.matches(proxyModel)) {
                getOrCreate(proxyModel);
                changes.getAndIncrement();
            }
        }

    }

    private void removeUsedProxies(List<ProxyModel> proxies, AtomicInteger changes) {
        Set<ProxyModel> cachedProxies = Sets.newHashSet(proxySamples.keySet());
        cachedProxies.removeAll(proxies);
        for(ProxyModel model : cachedProxies) {
            remove(model);
            changes.getAndIncrement();
        }
    }

    protected int getStartTime() {
        return 30 * 1000;
    }

    protected int getPeriodic() {
        return 60 * 1000;
    }

    private class ProxyInUseRuler implements Ruler<ProxyModel> {

        private Set<Long> proxyIds = Sets.newHashSet();

        private ProxyInUseRuler() {
            List<RouteModel> routes = routeService.getActiveRoutes();
            for(RouteModel route : routes) {
                if(!route.getTag().equalsIgnoreCase(Route.TAG_META)) {
                    continue;
                }
                addProxyIds(route);
            }
        }

        @Override
        public boolean matches(ProxyModel proxyModel) {
            return proxyIds.contains(proxyModel.getId());
        }

        private void addProxyIds(RouteModel route) {
            addProxyIds(route.getSrcProxyIds());
            addProxyIds(route.getDstProxyIds());
            addProxyIds(route.getOptionProxyIds());
        }

        private void addProxyIds(String idSet) {
            String[] ids = StringUtil.splitRemoveEmpty(",", idSet);
            for(String id : ids) {
                proxyIds.add(Long.parseLong(id));
            }
        }
    }

    private class TcpPortOnlyProxyRuler implements Ruler<ProxyModel> {

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

    @VisibleForTesting
    protected DefaultProxyMonitorCollectorManager setRouteService(RouteService routeService) {
        this.routeService = routeService;
        return this;
    }
}
