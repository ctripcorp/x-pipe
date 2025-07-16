package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.CrossRegionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CrossRegionSentinelHelloCollector;
import com.ctrip.xpipe.redis.core.meta.MetaCache;

import java.util.HashMap;
import java.util.Map;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;


public class CrossRegionSentinelCheckAggregationCollector extends AbstractAggregationCollector<CrossRegionSentinelHelloCollector> implements CrossRegionSupport, SentinelHelloCollector {

    private MetaCache metaCache;

    private final String dcId = FoundationService.DEFAULT.getDataCenter();

    public CrossRegionSentinelCheckAggregationCollector(MetaCache metaCache, CrossRegionSentinelHelloCollector sentinelHelloCollector, String clusterId, String shardId, CheckerConfig checkerConfig) {
        super(sentinelHelloCollector, clusterId, shardId, checkerConfig);
        this.metaCache = metaCache;
    }

    public boolean shouldCheckFromRedis(RedisHealthCheckInstance instance) {
        return instance.getCheckInfo().isCrossRegion() && !instance.getCheckInfo().isMaster() && metaCache.isCurrentDc(instance.getCheckInfo().getDcId());
    }

    protected boolean allInstancesCollected(int collectedInstanceCount) {
       return collectedInstanceCount >= metaCache.getRedisOfDcClusterShard(dcId, clusterId, shardId).size();
    }

    @Override
    public synchronized void onAction(SentinelActionContext context) {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        RedisInstanceInfo info = context.instance().getCheckInfo();
        transaction.logTransactionSwallowException("sentinel.check.notify", info.getClusterId(), new Task() {
            @Override
            public void go() throws Exception {
                if (!info.getClusterId().equalsIgnoreCase(clusterId) || !info.getShardId().equalsIgnoreCase(shardId))
                    return;

                if (!shouldCheckFromRedis(context.instance())) return;

                int collectedInstanceCount = collectHello(context);

                if (allInstancesCollected(collectedInstanceCount)) {
                    logger.debug("[{}-{}+{}]sub finish: {}", LOG_TITLE, clusterId, shardId, checkResult.toString());
                    handleAllHellos(context.instance());
                }
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("checkRedisInstances", info);
                return transactionData;
            }
        });
    }


    @Override
    protected boolean allInstancesCollectedInDowngradeStatus(int collectedInstanceCount) {
        return false;
    }

    @Override
    protected boolean allInstancesCollectedInNormalStatus(int collectedInstanceCount) {
        return false;
    }
}
