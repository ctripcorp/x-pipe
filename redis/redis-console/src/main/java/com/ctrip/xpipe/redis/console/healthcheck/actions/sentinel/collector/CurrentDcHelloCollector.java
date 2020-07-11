package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CurrentDcHelloCollector implements BiDirectionSupport, SentinelHelloCollector {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CurrentDcSentinelHelloCollector realCollector;

    Map<Pair<String, String>, CurrentDcSentinelHelloAggregationCollector> collectors = Maps.newConcurrentMap();

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        getCheckCollectorController(info.getClusterId(), info.getShardId()).onAction(context);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {

    }

    private CurrentDcSentinelHelloAggregationCollector getCheckCollectorController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(collectors, key, new ObjectFactory<CurrentDcSentinelHelloAggregationCollector>() {
            @Override
            public CurrentDcSentinelHelloAggregationCollector create() {
                return new CurrentDcSentinelHelloAggregationCollector(metaCache, realCollector, cluster, shard);
            }
        });
    }

}
