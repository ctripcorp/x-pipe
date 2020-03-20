package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class SentinelCheckDowngradeController implements HealthCheckActionController, SentinelHelloCollector {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private Set<HostPort> checkFinishedInstance = new HashSet<>();

    private Set<HostPort> checkFailInstance = new HashSet<>();

    private Set<SentinelHello> checkResult = new HashSet<>();

    private AtomicBoolean needDowngrade = new AtomicBoolean(false);

    private MetaCache metaCache;

    private DefaultSentinelHelloCollector realCollector;

    private long lastDoCollectTime = System.currentTimeMillis();

    private final String clusterName;

    private final String shardName;

    public SentinelCheckDowngradeController(MetaCache metaCache, DefaultSentinelHelloCollector sentinelHelloCollector, String clusterName, String shardName) {
        this.metaCache = metaCache;
        this.realCollector = sentinelHelloCollector;
        this.clusterName = clusterName;
        this.shardName = shardName;
    }

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        if (tooLongNoCollect(instance) && needDowngrade.compareAndSet(true, false)) {
            logger.warn("[{}-{}][shouldCheck] too long no collect, cancel downgrade", clusterName, shardName);
        }
        return shouldCheckFromRedis(instance);
    }

    @Override
    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        if (!info.getClusterId().equalsIgnoreCase(clusterName) || !info.getShardId().equalsIgnoreCase(shardName)) return;
        if (!shouldCheckFromRedis(context.instance())) return;

        synchronized (this) {
            checkFinishedInstance.add(info.getHostPort());
            if (!context.isFail()) checkResult.addAll(context.getResult());
            else checkFailInstance.add(info.getHostPort());
        }

        if (needDowngrade.compareAndSet(true, false)) {
            logger.info("[{}-{}][onAction] sub from active dc redis {}", clusterName, shardName, info.getHostPort());
            doCollect(context.instance());
            return;
        }

        if (info.isInActiveDc()) return;

        if (checkFinishedInstance.size() >= countBackDcRedis()) {
            if (checkFinishedInstance.size() == checkFailInstance.size()) {
                logger.warn("[{}-{}][onAction] backup dc sub sentinel hello all fail, try to sub from active dc", clusterName, shardName);
                needDowngrade.set(true);
                resetCheckResult();
                return;
            }
            logger.debug("[{}-{}][onAction] sub from backup dc all finish", clusterName, shardName);
            doCollect(context.instance());
        }
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        // no nothing
    }

    private boolean shouldCheckFromRedis(RedisHealthCheckInstance instance) {
        return !instance.getRedisInstanceInfo().isMaster()
                && instance.getRedisInstanceInfo().isInActiveDc() == needDowngrade.get();
    }

    private boolean tooLongNoCollect(RedisHealthCheckInstance instance) {
        int sentinelHealthCheckInterval = instance.getHealthCheckConfig().getSentinelCheckIntervalMilli();
        return System.currentTimeMillis() - lastDoCollectTime > 3 * sentinelHealthCheckInterval;
    }

    private int countBackDcRedis() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta || StringUtil.isEmpty(clusterName)) return 0;

        int redisCnt = 0;
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getClusters().containsKey(clusterName)) continue;
            if (dcMeta.getClusters().get(clusterName).getActiveDc().equalsIgnoreCase(dcMeta.getId())) continue;
            redisCnt += dcMeta.getClusters().get(clusterName).getShards().get(shardName).getRedises().size();
        }

        return redisCnt;
    }

    private void doCollect(RedisHealthCheckInstance instance) {
        Set<SentinelHello> hellos = new HashSet<>(checkResult);
        resetCheckResult();
        lastDoCollectTime = System.currentTimeMillis();
        this.realCollector.onAction(new SentinelActionContext(instance, hellos));
    }

    private synchronized void resetCheckResult() {
        this.checkFinishedInstance.clear();
        this.checkFailInstance.clear();
        this.checkResult.clear();
    }

}
