package com.ctrip.xpipe.redis.console.healthcheck.availablezone;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Detect redis instances without AZ in configured DCs and report via Cat Event + clog warn.
 */
@Component
public class RedisVacantAzChecker extends AbstractCrossDcIntervalAction {

    protected static final String REDIS_VACANT_AZ_EVENT_TYPE = "redis.vacant.az";

    @Autowired
    private MetaCache metaCache;

    private EventMonitor eventMonitor = EventMonitor.DEFAULT;

    private final List<ALERT_TYPE> alertType = Collections.emptyList();

    @Override
    protected void doAction() {
        Set<String> checkDcs = consoleConfig.getRedisVacantAzCheckDcs();
        if (checkDcs == null || checkDcs.isEmpty()) {
            logger.debug("[doAction] vacant az check dcs empty, skip");
            return;
        }

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null || xpipeMeta.getDcs() == null) {
            return;
        }

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (dcMeta == null || StringUtil.isEmpty(dcMeta.getId())) {
                continue;
            }
            if (!containsIgnoreCase(checkDcs, dcMeta.getId())) {
                continue;
            }
            checkDc(dcMeta);
        }
    }

    private boolean containsIgnoreCase(Set<String> checkDcs, String dcId) {
        for (String configuredDc : checkDcs) {
            if (configuredDc != null && configuredDc.equalsIgnoreCase(dcId)) {
                return true;
            }
        }
        return false;
    }

    private void checkDc(DcMeta dcMeta) {
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            if (clusterMeta == null || clusterMeta.getShards() == null) {
                continue;
            }
            for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                if (shardMeta == null || shardMeta.getRedises() == null) {
                    continue;
                }
                for (RedisMeta redisMeta : shardMeta.getRedises()) {
                    if (isVacantAz(redisMeta)) {
                        reportVacantAz(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), redisMeta);
                    }
                }
            }
        }
    }

    private boolean isVacantAz(RedisMeta redisMeta) {
        return redisMeta != null && StringUtil.isEmpty(redisMeta.getAz());
    }

    private void reportVacantAz(String dc, String clusterName, String shardName, RedisMeta redisMeta) {
        String ip = redisMeta.getIp();
        Integer port = redisMeta.getPort();

        Map<String, String> tags = new HashMap<>();
        tags.put("dc", dc == null ? "" : dc);
        tags.put("cluster", clusterName == null ? "" : clusterName);
        tags.put("shard", shardName == null ? "" : shardName);
        tags.put("ip", ip == null ? "" : ip);
        tags.put("port", port == null ? "" : String.valueOf(port));
        eventMonitor.logEvent(REDIS_VACANT_AZ_EVENT_TYPE, clusterName, tags);

        logger.warn("[doAction] redis vacant az, dc={}, cluster={}, shard={}, ip={}, port={}",
                dc, clusterName, shardName, ip, port);
    }

    @Override
    protected long getIntervalMilli() {
        return consoleConfig.getRedisVacantAzCheckIntervalMilli();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }

    @VisibleForTesting
    protected void setEventMonitor(EventMonitor eventMonitor) {
        this.eventMonitor = eventMonitor;
    }

    @VisibleForTesting
    protected void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }
}
