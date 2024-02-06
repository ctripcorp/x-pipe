package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.NoRedisToSubContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator.OneWaySentinelCheckAggregationCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
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
        String azGroupType = instance.getCheckInfo().getAzGroupType();
        if (!StringUtil.isEmpty(azGroupType) && ClusterType.lookup(azGroupType) == ClusterType.SINGLE_DC) {
            return false;
        }
        if (metaCache.isCrossRegion(instance.getCheckInfo().getActiveDc(), instance.getCheckInfo().getDcId())) {
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
        if (context instanceof NoRedisToSubContext) {
            getCheckCollectorController(((NoRedisToSubContext) context).getCluster(),
                    ((NoRedisToSubContext) context).getShard()).onAction(context);
        } else {
            RedisInstanceInfo info = context.instance().getCheckInfo();
            getCheckCollectorController(info.getClusterId(), info.getShardId())
                    .onAction(context);
        }
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
