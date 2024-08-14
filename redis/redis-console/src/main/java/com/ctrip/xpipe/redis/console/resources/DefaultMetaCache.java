package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.RedisCheckRuleService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisCheckRuleMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public class DefaultMetaCache extends AbstractMetaCache implements MetaCache, ConsoleLeaderAware {

    private int refreshIntervalMilli = 2000;

    @Autowired
    private RedisCheckRuleService redisCheckRuleService;

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private KeeperContainerService keeperContainerService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private RouteChooseStrategyFactory routeChooseStrategyFactory;

    private RouteChooseStrategy strategy = null;

    private List<Set<String>> clusterParts;

    private List<Set<Long>> keeperContainerParts;

    private List<TimeBoundCache<String>> xmlFormatXPipeMetaParts = null;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

    private ScheduledFuture<?> future;

    private AtomicBoolean taskTrigger = new AtomicBoolean(false);

    @Override
    public void isleader() {
        if (taskTrigger.compareAndSet(false, true)) {
            stopLoadMeta();
            startLoadMeta();
        }
    }

    @Override
    public void notLeader() {
        if (taskTrigger.compareAndSet(true, false))
            stopLoadMeta();
    }

    private synchronized void stopLoadMeta(){
        logger.info("[loadMeta][stop]{}", this);
        if (future != null)
            future.cancel(true);
        future = null;

        clusterParts = null;
        meta = null;
        monitor2ClusterShard = null;
        allKeepers = null;
        allKeeperSize = DEFAULT_KEEPER_NUMBERS;
        lastUpdateTime = 0;
    }

    public void startLoadMeta() {
        logger.info("[loadMeta][start]{}", this);

        refreshIntervalMilli = consoleConfig.getCacheRefreshInterval();

        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                if(!taskTrigger.get())
                    return;
                // loadCache();
            }

        }, 1000, refreshIntervalMilli, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void shutdown() {
        if(scheduled != null) {
            scheduled.shutdownNow();
        }
    }

    void loadCache() throws Exception {

        TransactionMonitor.DEFAULT.logTransaction("MetaCache", "load", new Task() {

            @Override
            public void go() throws Exception {

                Map<String, DcMeta> dcMetaMap = dcMetaService.getAllDcMetas();
                List<DcMeta> dcMetas = new ArrayList<>(dcMetaMap.values());

                List<RedisCheckRuleTbl> redisCheckRuleTbls = redisCheckRuleService.getAllRedisCheckRules();
                List<RedisCheckRuleMeta> redisCheckRuleMetas = new LinkedList<>();

                for(RedisCheckRuleTbl redisCheckRuleTbl : redisCheckRuleTbls) {
                    RedisCheckRuleMeta redisCheckRuleMeta = new RedisCheckRuleMeta();
                    redisCheckRuleMeta.setId(redisCheckRuleTbl.getId())
                            .setCheckType(redisCheckRuleTbl.getCheckType())
                            .setParam(redisCheckRuleTbl.getParam());

                    redisCheckRuleMetas.add(redisCheckRuleMeta);
                }

                synchronized (this) {
                    refreshMetaParts();
                    XpipeMeta xpipeMeta = createXpipeMeta(dcMetas, redisCheckRuleMetas);
                    refreshMeta(xpipeMeta);
                }
            }

            @Override
            public Map getData() {
                return null;
            }
        });
    }

    private void refreshMetaParts() {
        try {
            int parts = Math.max(1, consoleConfig.getClusterDividedParts());
            logger.debug("[refreshClusterParts] start parts {}", parts);

            List<Set<String>> newClusterParts = clusterService.divideClusters(parts);
            if (newClusterParts.size() < parts) {
                logger.info("[refreshClusterParts] skip for parts miss, expect {}, actual {}", parts, newClusterParts.size());
                return;
            }
            List<Set<Long>> newKeeperContainerParts = keeperContainerService.divideKeeperContainers(parts);
            if (newKeeperContainerParts.size() < parts) {
                logger.info("[refreshKeeperContainerParts] skip for parts miss, expect {}, actual {}",
                        parts, newKeeperContainerParts.size());
                return;
            }

            this.clusterParts = newClusterParts;
            this.keeperContainerParts = newKeeperContainerParts;

            List<TimeBoundCache<String>> localXPipeMetaParts = new ArrayList<>();
            IntStream.range(0, consoleConfig.getClusterDividedParts()).forEach(i -> {
                // using as lazy-load cache
                localXPipeMetaParts.add(new TimeBoundCache<>(() -> Long.MAX_VALUE, () -> getDividedXpipeMeta(i).toString()));
            });
            this.xmlFormatXPipeMetaParts = localXPipeMetaParts;
        } catch (Throwable th) {
            logger.warn("[refreshClusterParts] fail", th);
        }
    }

    @Override
    public Map<String, RouteMeta> chooseDefaultMetaRoutes(String clusterName, String srcDc, List<String> dstDcs) {
        return chooseMetaRoutes(clusterName, srcDc, dstDcs, false);
    }

    @Override
    public Map<String, RouteMeta> chooseClusterMetaRoutes(String clusterName, String srcDc, List<String> dstDcs) {
        return chooseMetaRoutes(clusterName, srcDc, dstDcs, true);
    }

    private Map<String, RouteMeta> chooseMetaRoutes(String clusterName, String srcDc, List<String> dstDcs, boolean useClusterPrioritizedRoutes) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        RouteChooseStrategyFactory.RouteStrategyType routeStrategyType =
                RouteChooseStrategyFactory.RouteStrategyType.lookup(consoleConfig.getChooseRouteStrategyType());

        return xpipeMetaManager.chooseMetaRoutes(clusterName, srcDc, dstDcs, getRouteChooseStrategy(routeStrategyType), useClusterPrioritizedRoutes);
    }

    private RouteChooseStrategy getRouteChooseStrategy(RouteChooseStrategyFactory.RouteStrategyType routeStrategyType) {
        RouteChooseStrategy localStrategy = strategy;
        if(null == localStrategy || !ObjectUtils.equals(routeStrategyType, localStrategy.getRouteStrategyType())) {
            localStrategy = routeChooseStrategyFactory.create(routeStrategyType);
            strategy = localStrategy;
        }

        return localStrategy;
    }

    @Override
    public synchronized XpipeMeta getDividedXpipeMeta(int partIndex) {
        if (null == meta || null == clusterParts) throw new DataNotFoundException("data not ready");
        if (partIndex >= clusterParts.size() || partIndex >= keeperContainerParts.size())
            throw new DataNotFoundException("no part " + partIndex);

        XpipeMeta xpipeMeta = getXpipeMeta();
        Set<String> requestClusters = clusterParts.get(partIndex);
        Set<Long> requestKeeperContainers = keeperContainerParts.get(partIndex);


        return createDividedMeta(xpipeMeta, requestClusters, requestKeeperContainers);
    }

    @Override
    public String getXmlFormatDividedXpipeMeta(int partIndex) {
        List<TimeBoundCache<String>> localParts = this.xmlFormatXPipeMetaParts;
        if (null == localParts) throw new DataNotFoundException("data not ready");
        if (partIndex >= localParts.size()) {
            throw new DataNotFoundException("no part " + partIndex);
        }

        return localParts.get(partIndex).getData();
    }

    @VisibleForTesting
    ScheduledFuture<?> getFuture() {
        return future;
    }

    @VisibleForTesting
    AtomicBoolean getTaskTrigger() {
        return taskTrigger;
    }
}
