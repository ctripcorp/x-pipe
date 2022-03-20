package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator.CurrentDcSentinelHelloAggregationCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CurrentDcSentinelHelloCheckController implements BiDirectionSupport, SingleDcSupport, LocalDcSupport, SentinelHelloCollector, SentinelActionController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CurrentDcSentinelHelloCollector realCollector;

    @Autowired
    private CheckerConfig config;

    Map<Pair<String, String>, CurrentDcSentinelHelloAggregationCollector> collectors = Maps.newConcurrentMap();

    @Override
    public void stopWatch(HealthCheckAction action) {
        for (CurrentDcSentinelHelloAggregationCollector collector : collectors.values())
            collector.stopWatch(action);
        collectors.clear();
    }

    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        return getCheckCollectorController(info.getClusterId(), info.getShardId()).shouldCheckFromRedis(instance);
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        getCheckCollectorController(info.getClusterId(), info.getShardId()).onAction(context);
    }

    private CurrentDcSentinelHelloAggregationCollector getCheckCollectorController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(collectors, key, new ObjectFactory<CurrentDcSentinelHelloAggregationCollector>() {
            @Override
            public CurrentDcSentinelHelloAggregationCollector create() {
                return new CurrentDcSentinelHelloAggregationCollector(metaCache, realCollector, cluster, shard, config);
            }
        });
    }

    @VisibleForTesting
    Map<Pair<String, String>, CurrentDcSentinelHelloAggregationCollector> getCollectors() {
        return collectors;
    }

    @VisibleForTesting
    void addCollector(String cluster, String shard, CurrentDcSentinelHelloAggregationCollector collector) {
        this.collectors.put(new Pair<>(cluster, shard), collector);
    }
}
