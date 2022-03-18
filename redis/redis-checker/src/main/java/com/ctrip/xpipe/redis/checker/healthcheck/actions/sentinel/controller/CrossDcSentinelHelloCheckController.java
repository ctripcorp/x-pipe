package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.CrossDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CrossDcSentinelHellosCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator.CrossDcSentinelHelloAggregationCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CrossDcSentinelHelloCheckController implements CrossDcSupport, SentinelHelloCollector, SentinelActionController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CrossDcSentinelHellosCollector realCollector;

    @Autowired
    private CheckerConfig checkerConfig;

    Map<Pair<String, String>, CrossDcSentinelHelloAggregationCollector> collectors = Maps.newConcurrentMap();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return getCheckCollectorController(instance.getCheckInfo().getClusterId(), instance.getCheckInfo().getShardId()).shouldCheckFromRedis(instance);
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        getCheckCollectorController(info.getClusterId(), info.getShardId()).onAction(context);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        for (CrossDcSentinelHelloAggregationCollector collector : collectors.values())
            collector.stopWatch(action);
        collectors.clear();
    }

    private CrossDcSentinelHelloAggregationCollector getCheckCollectorController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(collectors, key, new ObjectFactory<CrossDcSentinelHelloAggregationCollector>() {
            @Override
            public CrossDcSentinelHelloAggregationCollector create() {
                return new CrossDcSentinelHelloAggregationCollector(metaCache, realCollector, cluster, shard, checkerConfig);
            }
        });
    }

    @VisibleForTesting
    Map<Pair<String, String>, CrossDcSentinelHelloAggregationCollector> getCollectors() {
        return collectors;
    }

    @VisibleForTesting
    void addCollector(String cluster, String shard, CrossDcSentinelHelloAggregationCollector collector) {
        this.collectors.put(new Pair<>(cluster, shard), collector);
    }
}
