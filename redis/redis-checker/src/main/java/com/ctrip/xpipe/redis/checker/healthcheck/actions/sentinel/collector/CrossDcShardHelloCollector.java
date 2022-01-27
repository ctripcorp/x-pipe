package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.redis.checker.healthcheck.CrossDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CrossDcShardHelloCollector extends AbstractAggregationCollector<CrossDcSentinelHellosAnalyzer> implements CrossDcSupport {

    protected static Logger logger = LoggerFactory.getLogger(CrossDcShardHelloCollector.class);

    private MetaCache metaCache;

    public CrossDcShardHelloCollector(MetaCache metaCache, CrossDcSentinelHellosAnalyzer sentinelHelloCollector,
                                                      String clusterId, String shardId) {
        super(sentinelHelloCollector, clusterId, shardId);
        this.metaCache = metaCache;
    }

    @Override
    public void onAction(SentinelActionContext context) {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        RedisInstanceInfo info = context.instance().getCheckInfo();
        transaction.logTransactionSwallowException("sentinel.check.notify", info.getClusterId(), new Task() {
            @Override
            public void go() throws Exception {
                RedisInstanceInfo info = context.instance().getCheckInfo();
                if (!info.getClusterId().equalsIgnoreCase(clusterId) || !info.getShardId().equalsIgnoreCase(shardId))
                    return;

                if (collectHello(context) >= getSlavesCntInCurrentShard()) {
                    if (checkFinishedInstance.size() == checkFailInstance.size()) {
                        logger.info("[{}-{}][onAction] sentinel hello all fail, skip sentinel adjust", clusterId, shardId);
                        resetCheckResult();
                        return;
                    }
                    logger.debug("[{}-{}][onAction] sentinel hello collect finish: {}", clusterId, shardId, checkResult.toString());
                    handleAllBackupDcHellos(context.instance());
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

    private int getSlavesCntInCurrentShard() {
        return metaCache.getSlavesOfShard(clusterId, shardId).size();
    }

}

