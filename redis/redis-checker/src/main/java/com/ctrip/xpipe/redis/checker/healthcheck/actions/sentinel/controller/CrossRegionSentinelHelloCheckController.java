package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator.CrossRegionSentinelCheckAggregationCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CrossRegionSentinelHelloCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;


@Component
public class CrossRegionSentinelHelloCheckController implements CrossRegionSupport, SentinelHelloCollector, SentinelActionController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    @Qualifier("crossRegionSentinelHelloCollector")
    private CrossRegionSentinelHelloCollector sentinelAdjuster;

    private Map<Pair<String, String>, CrossRegionSentinelCheckAggregationCollector> controllerMap = Maps.newConcurrentMap();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        if (!(metaCache.isCrossRegion(instance.getCheckInfo().getActiveDc(), instance.getCheckInfo().getDcId()) && metaCache.isCurrentDc(instance.getCheckInfo().getDcId()))) {
            return false;
        }

        return getCheckCollectorController(info.getClusterId(), info.getShardId()).shouldCheckFromRedis(instance);
    }

    @Override
    public boolean worksfor(ActionContext t) {
        return t instanceof SentinelActionContext;
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        getCheckCollectorController(info.getClusterId(), info.getShardId())
                .onAction(context);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        for (CrossRegionSentinelCheckAggregationCollector collector : controllerMap.values())
            collector.stopWatch(action);
        controllerMap.clear();
    }

    private CrossRegionSentinelCheckAggregationCollector getCheckCollectorController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(controllerMap, key, new ObjectFactory<CrossRegionSentinelCheckAggregationCollector>() {
            @Override
            public CrossRegionSentinelCheckAggregationCollector create() {
                return new CrossRegionSentinelCheckAggregationCollector(metaCache, sentinelAdjuster, cluster, shard, checkerConfig);
            }
        });
    }

    @VisibleForTesting
    protected void addCheckCollectorController(String cluster, String shard, CrossRegionSentinelCheckAggregationCollector collectorController) {
        controllerMap.put(Pair.of(cluster, shard), collectorController);
    }

    @VisibleForTesting
    protected Map<Pair<String, String>, CrossRegionSentinelCheckAggregationCollector> getAllCheckCollectorControllers() {
        return controllerMap;
    }

}
