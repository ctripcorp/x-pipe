package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.CrossDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CrossDcClusterHelloCollector implements CrossDcSupport, SentinelHelloCollector {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CrossDcSentinelHellosAnalyzer realCollector;

    Map<Pair<String, String>, CrossDcShardHelloCollector> collectors = Maps.newConcurrentMap();

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        getCheckCollectorController(info.getClusterId(), info.getShardId()).onAction(context);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {

    }

    private CrossDcShardHelloCollector getCheckCollectorController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(collectors, key, new ObjectFactory<CrossDcShardHelloCollector>() {
            @Override
            public CrossDcShardHelloCollector create() {
                return new CrossDcShardHelloCollector(metaCache, realCollector, cluster, shard);
            }
        });
    }

}
