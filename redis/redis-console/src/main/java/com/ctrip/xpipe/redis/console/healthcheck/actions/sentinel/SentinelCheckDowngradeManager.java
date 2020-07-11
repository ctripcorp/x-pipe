package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;


@Component
public class SentinelCheckDowngradeManager implements OneWaySupport, SentinelHelloCollector, SentinelActionController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    @Qualifier("defaultSentinelHelloCollector")
    private DefaultSentinelHelloCollector sentinelAdjuster;

    private Map<Pair<String, String>, SentinelCheckDowngradeCollectorController> controllerMap = Maps.newConcurrentMap();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        return getCheckCollectorController(info.getClusterId(), info.getShardId()).shouldCheck(instance);
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        getCheckCollectorController(info.getClusterId(), info.getShardId())
                .onAction(context);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {

    }

    private SentinelCheckDowngradeCollectorController getCheckCollectorController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(controllerMap, key, new ObjectFactory<SentinelCheckDowngradeCollectorController>() {
            @Override
            public SentinelCheckDowngradeCollectorController create() {
                return new SentinelCheckDowngradeCollectorController(metaCache, sentinelAdjuster, cluster, shard);
            }
        });
    }

    @VisibleForTesting
    protected void addCheckCollectorController(String cluster, String shard, SentinelCheckDowngradeCollectorController collectorController) {
        controllerMap.put(Pair.of(cluster, shard), collectorController);
    }

}
