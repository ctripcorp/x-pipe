package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator.OneWaySentinelCheckAggregationCollector;
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
public class OneWaySentinelHelloCheckController implements OneWaySupport, SentinelHelloCollector, SentinelActionController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    @Qualifier("defaultSentinelHelloCollector")
    private DefaultSentinelHelloCollector sentinelAdjuster;

    private Map<Pair<String, String>, OneWaySentinelCheckAggregationCollector> controllerMap = Maps.newConcurrentMap();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        if (!DcGroupType.isNullOrDrMaster(info.getDcGroupType())) return false;

        return getCheckCollectorController(info.getClusterId(), info.getShardId()).shouldCheckFromRedis(instance);
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        getCheckCollectorController(info.getClusterId(), info.getShardId())
                .onAction(context);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        for (OneWaySentinelCheckAggregationCollector collector : controllerMap.values())
            collector.stopWatch(action);
        controllerMap.clear();
    }

    private OneWaySentinelCheckAggregationCollector getCheckCollectorController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(controllerMap, key, new ObjectFactory<OneWaySentinelCheckAggregationCollector>() {
            @Override
            public OneWaySentinelCheckAggregationCollector create() {
                return new OneWaySentinelCheckAggregationCollector(metaCache, sentinelAdjuster, cluster, shard, checkerConfig);
            }
        });
    }

    @VisibleForTesting
    protected void addCheckCollectorController(String cluster, String shard, OneWaySentinelCheckAggregationCollector collectorController) {
        controllerMap.put(Pair.of(cluster, shard), collectorController);
    }

    @VisibleForTesting
    protected Map<Pair<String, String>, OneWaySentinelCheckAggregationCollector> getAllCheckCollectorControllers() {
        return controllerMap;
    }

}
