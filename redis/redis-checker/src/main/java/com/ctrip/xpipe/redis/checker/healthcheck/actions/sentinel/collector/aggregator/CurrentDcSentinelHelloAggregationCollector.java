package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.LocalDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.SingleDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurrentDcSentinelHelloAggregationCollector extends AbstractAggregationCollector<CurrentDcSentinelHelloCollector> implements BiDirectionSupport, SingleDcSupport, LocalDcSupport {
    protected static Logger logger = LoggerFactory.getLogger(CurrentDcSentinelHelloAggregationCollector.class);

    private MetaCache metaCache;

    private final String dcId = FoundationService.DEFAULT.getDataCenter();

    public CurrentDcSentinelHelloAggregationCollector(MetaCache metaCache, CurrentDcSentinelHelloCollector sentinelHelloCollector,
                                                      String clusterId, String shardId, CheckerConfig checkerConfig) {
        super(sentinelHelloCollector, clusterId, shardId, checkerConfig);
        this.metaCache = metaCache;
    }

    @Override
    public boolean shouldCheckFromRedis(RedisHealthCheckInstance instance) {
        return needDowngradeSubBothMasterAndSlavesInCurrentDc(instance) ||
                noNeedDowngradeSubOnlySlavesInCurrentDc(instance);
    }

    private boolean needDowngradeSubBothMasterAndSlavesInCurrentDc(RedisHealthCheckInstance instance) {
        return needDowngrade.get() && instance.getCheckInfo().getDcId().equalsIgnoreCase(dcId);
    }

    private boolean noNeedDowngradeSubOnlySlavesInCurrentDc(RedisHealthCheckInstance instance) {
        return !needDowngrade.get() && !instance.getCheckInfo().isMaster() &&
                instance.getCheckInfo().getDcId().equalsIgnoreCase(dcId);
    }

    @Override
    protected boolean allInstancesCollectedInDowngradeStatus(int collectedInstanceCount) {
        return collectedInstanceCount >= countAllInstancesInCurrentDc();
    }

    @Override
    protected boolean allInstancesCollectedInNormalStatus(int collectedInstanceCount) {
        return collectedInstanceCount >= countAllSlavesInCurrentDc();
    }

    private int countAllInstancesInCurrentDc() {
        return metaCache.getRedisOfDcClusterShard(dcId, clusterId, shardId).size();
    }

    private int countAllSlavesInCurrentDc() {
        return metaCache.getSlavesOfDcClusterShard(dcId, clusterId, shardId).size();
    }


}
