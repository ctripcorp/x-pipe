package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;


import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HeteroSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator.CurrentDcSentinelHelloAggregationCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator.OneWaySentinelCheckAggregationCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class HeteroSentinelHelloCheckController implements HeteroSupport, SentinelHelloCollector, SentinelActionController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    private DefaultSentinelHelloCollector defaultSentinelHelloCollector;

    @Autowired
    private CurrentDcSentinelHelloCollector currentDcSentinelHelloCollector;

    private Map<Pair<String, String>, SentinelHelloCollector> controllerMap = Maps.newConcurrentMap();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        if (isMasterTypeDcGroup(info.getClusterId(), info.getShardId()))
            return ((CurrentDcSentinelHelloAggregationCollector) getCheckCollectorController(info.getClusterId(), info.getShardId())).shouldCheckFromRedis(instance);
        else
            return ((OneWaySentinelCheckAggregationCollector) getCheckCollectorController(info.getClusterId(), info.getShardId())).shouldCheckFromRedis(instance);
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        getCheckCollectorController(info.getClusterId(), info.getShardId())
                .onAction(context);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        for (SentinelHelloCollector collector : controllerMap.values())
            collector.stopWatch(action);
        controllerMap.clear();
    }

    private SentinelHelloCollector getCheckCollectorController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(controllerMap, key, new ObjectFactory<SentinelHelloCollector>() {
            @Override
            public SentinelHelloCollector create() {
                return isMasterTypeDcGroup(cluster, shard) ?
                        new CurrentDcSentinelHelloAggregationCollector(metaCache, currentDcSentinelHelloCollector, cluster, shard, checkerConfig) :
                        new OneWaySentinelCheckAggregationCollector(metaCache, defaultSentinelHelloCollector, cluster, shard, checkerConfig);
            }
        });
    }

    private boolean isMasterTypeDcGroup(String cluster, String shard) {
//        todo: isMasterType
        return false;
    }


}
