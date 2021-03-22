package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.AbstractAggregationCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import java.util.concurrent.atomic.AtomicBoolean;

public class SentinelCheckDowngradeCollectorController extends AbstractAggregationCollector<DefaultSentinelHelloCollector> implements OneWaySupport, SentinelActionController {

    private AtomicBoolean needDowngrade = new AtomicBoolean(false);

    private MetaCache metaCache;

    // avoid always do downgrade without recovery
    private volatile long downgradeBeginTime = 0;

    public SentinelCheckDowngradeCollectorController(MetaCache metaCache, DefaultSentinelHelloCollector sentinelHelloCollector, String clusterId, String shardId) {
        super(sentinelHelloCollector, clusterId, shardId);
        this.metaCache = metaCache;
    }

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        if (tooLongNoCollect(instance) && needDowngrade.compareAndSet(true, false)) {
            logger.warn("[{}-{}][shouldCheck] too long no collect, cancel downgrade", clusterId, shardId);
        }
        return shouldCheckFromRedis(instance);
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        if (!info.getClusterId().equalsIgnoreCase(clusterId) || !info.getShardId().equalsIgnoreCase(shardId)) return;
        if (!shouldCheckFromRedis(context.instance())) return;

        // only deal with success result when downgrade
        if (!context.isFail() && needDowngrade.compareAndSet(true, false)) {
            logger.info("[{}-{}][onAction] sub from active dc redis {}", clusterId, shardId, info.getHostPort());
            handleAllHello(context.instance());
            return;
        }

        // handle backup dc hello when all right
        if (info.isInActiveDc()) return;
        if (collectHello(context) >= countBackDcRedis()) {
            if (checkFinishedInstance.size() == checkFailInstance.size()) {
                logger.warn("[{}-{}][onAction] backup dc sub sentinel hello all fail, try to sub from active dc", clusterId, shardId);
                beginDowngrade();
                return;
            }
            logger.debug("[{}-{}][onAction] sub from backup dc all finish", clusterId, shardId);
            handleAllHello(context.instance());
        }
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        // no nothing
    }

    private boolean shouldCheckFromRedis(RedisHealthCheckInstance instance) {
        return !instance.getCheckInfo().isMaster()
                && instance.getCheckInfo().isInActiveDc() == needDowngrade.get();
    }

    private boolean tooLongNoCollect(RedisHealthCheckInstance instance) {
        int sentinelHealthCheckInterval = instance.getHealthCheckConfig().getSentinelCheckIntervalMilli();
        return System.currentTimeMillis() - downgradeBeginTime > 3 * sentinelHealthCheckInterval;
    }

    private int countBackDcRedis() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta || StringUtil.isEmpty(clusterId)) return 0;

        int redisCnt = 0;
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getClusters().containsKey(clusterId)) continue;
            if (dcMeta.getClusters().get(clusterId).getActiveDc().equalsIgnoreCase(dcMeta.getId())) continue;
            redisCnt += dcMeta.getClusters().get(clusterId).getShards().get(shardId).getRedises().size();
        }

        return redisCnt;
    }

    private void beginDowngrade() {
        downgradeBeginTime = System.currentTimeMillis();
        needDowngrade.set(true);
        resetCheckResult();
    }

}
