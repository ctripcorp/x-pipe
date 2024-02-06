package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator;

import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.HashSet;
import java.util.Set;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class OneWaySentinelCheckAggregationCollector extends AbstractAggregationCollector<DefaultSentinelHelloCollector> implements OneWaySupport, SentinelHelloCollector {

    private MetaCache metaCache;

    public OneWaySentinelCheckAggregationCollector(MetaCache metaCache, DefaultSentinelHelloCollector sentinelHelloCollector, String clusterId, String shardId, CheckerConfig checkerConfig) {
        super(sentinelHelloCollector, clusterId, shardId, checkerConfig);
        this.metaCache = metaCache;
    }

    @Override
    protected boolean allInstancesCollectedInDowngradeStatus(int collectedInstanceCount) {
        return collectedInstanceCount >= countActiveDcRegionSlaves();
    }

    @Override
    protected boolean allInstancesCollectedInNormalStatus(int collectedInstanceCount) {
        return collectedInstanceCount >= countActiveDcRegionDRSlaves();
    }

    public boolean shouldCheckFromRedis(RedisHealthCheckInstance instance) {
        return noNeedDowngradeAndIsDrSlave(instance) || needDowngradeAndIsSlave(instance);
    }

    private boolean noNeedDowngradeAndIsDrSlave(RedisHealthCheckInstance instance) {
        boolean shouldCheck = !needDowngrade && !instance.getCheckInfo().isMaster() && !instance.getCheckInfo().isInActiveDc();

        logger.debug("[{}-{}+{}][{}]noNeedDowngradeAndIsDrSlave:{}, needDowngrade:{}, isMaster:{}, isInActiveDc:{}", LOG_TITLE, clusterId, shardId, instance.getCheckInfo().getHostPort(),
                shouldCheck, needDowngrade, instance.getCheckInfo().isMaster(), instance.getCheckInfo().isInActiveDc());

        return shouldCheck;
    }

    private boolean needDowngradeAndIsSlave(RedisHealthCheckInstance instance) {
        boolean shouldCheck = needDowngrade && !instance.getCheckInfo().isMaster();

        logger.debug("[{}-{}+{}][{}]needDowngradeAndIsSlave:{}, needDowngrade:{}, isMaster:{}", LOG_TITLE, clusterId, shardId, instance.getCheckInfo().getHostPort(),
                shouldCheck, needDowngrade, instance.getCheckInfo().isMaster());

        return shouldCheck;
    }

    private Set<DcMeta> getActiveDcRegionDcs() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta || StringUtil.isEmpty(clusterId)) return null;

        Set<DcMeta> dcs = new HashSet<>();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getClusters().containsKey(clusterId)) continue;
            if (metaCache.isCrossRegion(dcMeta.getClusters().get(clusterId).getActiveDc(), dcMeta.getId())) continue;
            dcs.add(dcMeta);
        }

        return dcs;
    }

    private int countActiveDcRegionDRSlaves() {
        Set<DcMeta> activeDcRegionDcs = getActiveDcRegionDcs();
        if (null == activeDcRegionDcs) return 0;

        int redisCnt = 0;
        for (DcMeta dcMeta : activeDcRegionDcs) {
            if (dcMeta.getClusters().get(clusterId).getActiveDc().equalsIgnoreCase(dcMeta.getId())) continue;
            ShardMeta shardMeta = dcMeta.findCluster(clusterId).findShard(shardId);
            if (null == shardMeta) continue; // cluster missing shard when no instances in it
            redisCnt += shardMeta.getRedises().size();
        }
        return redisCnt;
    }

    private int countActiveDcRegionSlaves() {
        Set<DcMeta> activeDcRegionDcs = getActiveDcRegionDcs();
        if (null == activeDcRegionDcs) return 0;

        int redisCnt = 0;
        for (DcMeta dcMeta : activeDcRegionDcs) {
            ShardMeta shardMeta = dcMeta.findCluster(clusterId).findShard(shardId);
            if (null == shardMeta) continue; // cluster missing shard when no instances in it
            redisCnt += shardMeta.getRedises().stream().filter(redisMeta -> !redisMeta.isMaster()).count();
        }
        return redisCnt;
    }

}
