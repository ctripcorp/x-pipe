package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator;

import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.CrossDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CrossDcSentinelHellosCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrossDcSentinelHelloAggregationCollector extends AbstractAggregationCollector<CrossDcSentinelHellosCollector> implements CrossDcSupport {

    protected static Logger logger = LoggerFactory.getLogger(CrossDcSentinelHelloAggregationCollector.class);

    private MetaCache metaCache;

    public CrossDcSentinelHelloAggregationCollector(MetaCache metaCache, CrossDcSentinelHellosCollector sentinelHelloCollector,
                                                    String clusterId, String shardId, CheckerConfig checkerConfig) {
        super(sentinelHelloCollector, clusterId, shardId, checkerConfig);
        this.metaCache = metaCache;
    }

    @Override
    public boolean shouldCheckFromRedis(RedisHealthCheckInstance instance) {
        return needDowngradeSubBothMasterAndSlaves() || noNeedDowngradeSubOnlySlaves(instance);
    }

    private boolean needDowngradeSubBothMasterAndSlaves() {
        return needDowngrade.get();
    }

    private boolean noNeedDowngradeSubOnlySlaves(RedisHealthCheckInstance instance) {
        return !needDowngrade.get() && !instance.getCheckInfo().isMaster();
    }

    @Override
    protected boolean allInstancesCollectedInDowngradeStatus(int collectedInstanceCount) {
        return collectedInstanceCount >= countAllInstancesInCurrentShard();
    }

    @Override
    protected boolean allInstancesCollectedInNormalStatus(int collectedInstanceCount) {
        return collectedInstanceCount >= countSlavesInCurrentShard();
    }

    private int countAllInstancesInCurrentShard() {
        return metaCache.getAllInstancesOfShard(clusterId, shardId).size();
    }

    private int countSlavesInCurrentShard() {
        return metaCache.getSlavesOfShard(clusterId, shardId).size();
    }

}

