package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderAware;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
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
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private ConsoleConfig consoleConfig;

    @Autowired
    private RouteChooseStrategyFactory routeChooseStrategyFactory;

    private RouteChooseStrategy strategy = null;

    private List<Set<String>> clusterParts;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

    @Override
    public void isleader() {
        loadMeta();
    }

    @Override
    public void notLeader() {
        shutdown();
    }

    public void loadMeta() {

        logger.info("[postConstruct]{}", this);

        refreshIntervalMilli = consoleConfig.getCacheRefreshInterval();

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                loadCache();
            }

        }, 1000, refreshIntervalMilli, TimeUnit.MILLISECONDS);
    }


    public void shutdown() {
        if(scheduled != null) {
            scheduled.shutdownNow();
        }
    }

    private void loadCache() throws Exception {


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


                refreshClusterParts();
                XpipeMeta xpipeMeta = createXpipeMeta(dcMetas, redisCheckRuleMetas);
                refreshMeta(xpipeMeta);
            }

            @Override
            public Map getData() {
                return null;
            }
        });
    }

    private void refreshClusterParts() {
        try {
            int parts = Math.max(1, consoleConfig.getClusterDividedParts());
            logger.debug("[refreshClusterParts] start parts {}", parts);

            List<Set<String>> newClusterParts = clusterService.divideClusters(parts);
            if (newClusterParts.size() < parts) {
                logger.info("[refreshClusterParts] skip for parts miss, expect {}, actual {}", parts, newClusterParts.size());
                return;
            }

            this.clusterParts = newClusterParts;
        } catch (Throwable th) {
            logger.warn("[refreshClusterParts] fail", th);
        }
    }

    @Override
    public Map<String, RouteMeta> chooseDefaultRoutes(String clusterName, String srcDc, List<String> dstDcs, int orgId) {
        return chooseRoutes(clusterName, srcDc, dstDcs, orgId, false);
    }

    @Override
    public Map<String, RouteMeta> chooseRoutes(String clusterName, String srcDc, List<String> dstDcs, int orgId) {
        return chooseRoutes(clusterName, srcDc, dstDcs, orgId, true);
    }

    private Map<String, RouteMeta> chooseRoutes(String clusterName, String srcDc, List<String> dstDcs, int orgId, boolean useClusterPrioritizedRoutes) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        RouteChooseStrategyFactory.RouteStrategyType routeStrategyType =
                RouteChooseStrategyFactory.RouteStrategyType.lookup(consoleConfig.getChooseRouteStrategyType());

        return xpipeMetaManager.chooseMetaRoutes(clusterName, srcDc, dstDcs, orgId, getRouteChooseStrategy(routeStrategyType), useClusterPrioritizedRoutes);
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
    public XpipeMeta getDividedXpipeMeta(int partIndex) {
        if (null == meta || null == clusterParts) throw new DataNotFoundException("data not ready");
        if (partIndex >= clusterParts.size()) throw new DataNotFoundException("no part " + partIndex);

        XpipeMeta xpipeMeta = getXpipeMeta();
        Set<String> requestClusters = clusterParts.get(partIndex);

        return createDividedMeta(xpipeMeta, requestClusters);
    }

}
