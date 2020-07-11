package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;

import java.util.List;

public class CurrentDcSentinelHelloAggregationCollector extends AbstractAggregationCollector<CurrentDcSentinelHelloCollector> implements BiDirectionSupport {

    private MetaCache metaCache;

    private final String dcId = FoundationService.DEFAULT.getDataCenter();

    public CurrentDcSentinelHelloAggregationCollector(MetaCache metaCache, CurrentDcSentinelHelloCollector sentinelHelloCollector,
                                                      String clusterId, String shardId) {
        super(sentinelHelloCollector, clusterId, shardId);
        this.metaCache = metaCache;
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        if (!info.getClusterId().equalsIgnoreCase(clusterId) || !info.getShardId().equalsIgnoreCase(shardId)) return;

        collectHello(context);

        if (checkFinishedInstance.size() >= getRedisCntInCurrentDc() - 1) {
            if (checkFinishedInstance.size() == checkFailInstance.size()) {
                logger.info("[{}-{}][onAction] sentinel hello all fail, skip sentinel adjust", clusterId, shardId);
                resetCheckResult();
                return;
            }
            logger.debug("[{}-{}][onAction] sentinel hello collect finish", clusterId, shardId);
            handleAllHello(context.instance());
        }
    }

    private int getRedisCntInCurrentDc() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta
                || !xpipeMeta.getDcs().containsKey(dcId)
                || !xpipeMeta.getDcs().get(dcId).getClusters().containsKey(clusterId)
                || !xpipeMeta.getDcs().get(dcId).getClusters().get(clusterId).getShards().containsKey(shardId)) {
            return 0;
        }

        List<RedisMeta> currentDcRediss = xpipeMeta.getDcs().get(dcId).getClusters().get(clusterId).getShards().get(shardId).getRedises();
        return null == currentDcRediss ? 0 : currentDcRediss.size();
    }

}
