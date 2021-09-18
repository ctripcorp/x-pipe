package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.LocalDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.SingleDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CurrentDcSentinelHelloAggregationCollector extends AbstractAggregationCollector<CurrentDcSentinelHelloCollector> implements BiDirectionSupport, SingleDcSupport, LocalDcSupport {
    protected static Logger logger = LoggerFactory.getLogger(CurrentDcSentinelHelloAggregationCollector.class);

    private MetaCache metaCache;

    private final String dcId = FoundationService.DEFAULT.getDataCenter();

    public CurrentDcSentinelHelloAggregationCollector(MetaCache metaCache, CurrentDcSentinelHelloCollector sentinelHelloCollector,
                                                      String clusterId, String shardId) {
        super(sentinelHelloCollector, clusterId, shardId);
        this.metaCache = metaCache;
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        if (!info.getClusterId().equalsIgnoreCase(clusterId) || !info.getShardId().equalsIgnoreCase(shardId)) return;

        if (collectHello(context) >= getRedisCntInCurrentDc() - 1) {
            if (checkFinishedInstance.size() == checkFailInstance.size()) {
                logger.info("[{}-{}][onAction] sentinel hello all fail, skip sentinel adjust", clusterId, shardId);
                resetCheckResult();
                return;
            }
            logger.debug("[{}-{}][onAction] sentinel hello collect finish", clusterId, shardId);
            handleAllBackupDcHellos(context.instance());
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
