package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class SentinelCheckControllerManager {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private DefaultSentinelHelloCollector sentinelAdjuster;

    private Map<Pair<String, String>, SentinelCheckDowngradeController> controllerMap = Maps.newConcurrentMap();

    public SentinelCheckDowngradeController getCheckController(String cluster, String shard) {
        Pair<String, String> key = new Pair<>(cluster, shard);
        return MapUtils.getOrCreate(controllerMap, key, new ObjectFactory<SentinelCheckDowngradeController>() {
            @Override
            public SentinelCheckDowngradeController create() {
                return new SentinelCheckDowngradeController(metaCache, sentinelAdjuster, cluster, shard);
            }
        });
    }

}
