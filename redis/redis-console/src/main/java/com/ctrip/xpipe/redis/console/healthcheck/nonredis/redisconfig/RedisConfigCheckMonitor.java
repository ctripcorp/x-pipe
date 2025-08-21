package com.ctrip.xpipe.redis.console.healthcheck.nonredis.redisconfig;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterServiceImpl;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class RedisConfigCheckMonitor extends AbstractCrossDcIntervalAction {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private DcService dcService;

    @Autowired
    private DcClusterServiceImpl dcClusterService;

    private static final String ACTIVE_DEFAULT_REDIS_CHECK_RULE = "active.default.redis.check.rule";

    @Override
    protected boolean shouldDoAction() {
        if(super.shouldDoAction() && consoleConfig.isRedisConfigCheckMonitorOpen() && !StringUtil.isEmpty(consoleConfig.getRedisConfigCheckRules())) {
            return true;
        }
        return false;
    }

    @Override
    protected void doAction() {
        Set<String> redisConfigCheckRule = Sets.newHashSet(consoleConfig.getRedisConfigCheckRules().split(","));
        Map<String, Long> dcNameZoneMap = dcService.dcNameZoneMap();

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        for(DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            for(ClusterMeta clustMeta : dcMeta.getClusters().values()) {
                if(isBiDirectionAndCrossRegionCluster(clustMeta, dcNameZoneMap)) {
                    addRedisConfigCheckRulesToDCCluster(clustMeta, dcMeta, redisConfigCheckRule);
                }
            }
        }
    }

    private void addRedisConfigCheckRulesToDCCluster(ClusterMeta clustMeta, DcMeta dcMeta, Set<String> readyToAddRedisConfigRules) {
        DcClusterCreateInfo dcClusterCreateInfo = dcClusterService.findDcClusterCreateInfo(dcMeta.getId(), clustMeta.getId());
        String newRedisConfigCheckRule = generateNewRedisConfigCheckRule(dcClusterCreateInfo.getRedisCheckRule(), readyToAddRedisConfigRules);
        if(newRedisConfigCheckRule != null) {
            updateRedisConfigCheckRules(clustMeta.getId(), dcMeta.getId(), dcClusterCreateInfo, newRedisConfigCheckRule);
        }
    }



    @VisibleForTesting
    protected String generateNewRedisConfigCheckRule(String oldRedisConfigRule, Set<String> readyToAddRedisConfigRule) {
        if(StringUtil.isEmpty(oldRedisConfigRule))
            return StringUtil.join(",", (arg)->(arg), readyToAddRedisConfigRule);

        Set<String> oldRedisConfigCheckRules = Sets.newHashSet(oldRedisConfigRule.split(","));
        if(oldRedisConfigCheckRules.containsAll(readyToAddRedisConfigRule))
            return null;

        Set<String> newRedisConfigCheckRules = new HashSet<>();
        newRedisConfigCheckRules.addAll(oldRedisConfigCheckRules);
        newRedisConfigCheckRules.addAll(readyToAddRedisConfigRule);

        return StringUtil.join(",", (arg)->(arg), newRedisConfigCheckRules);
    }

    private void updateRedisConfigCheckRules(String clusterId, String dcId, DcClusterCreateInfo dcClusterCreateInfo, String newRedisConfigCheckRule) {
        String oldRedisCheckRule = dcClusterCreateInfo.getRedisCheckRule();
        dcClusterCreateInfo.setRedisCheckRule(newRedisConfigCheckRule);
        dcClusterService.updateDcCluster(dcClusterCreateInfo);
        CatEventMonitor.DEFAULT.logEvent(ACTIVE_DEFAULT_REDIS_CHECK_RULE, String.format("redis check rule of dc:%s cluster:%s was changed from %s to %s",
                dcId, clusterId, oldRedisCheckRule, newRedisConfigCheckRule));
    }

    @VisibleForTesting
    protected boolean isBiDirectionAndCrossRegionCluster(ClusterMeta clusterMeta, Map<String, Long> dcNameZoneMap) {
        if(!ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.BI_DIRECTION))
            return false;

        List<String> dcNames = Arrays.asList(clusterMeta.getDcs().split(","));
        Set<Long> clusterZones = new HashSet<>();
        for(String dcName : dcNames) {
            clusterZones.add(dcNameZoneMap.get(dcName));
        }

        if(clusterZones.size() > 1)
            return true;
        return false;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.emptyList();
    }
}
