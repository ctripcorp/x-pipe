package com.ctrip.xpipe.redis.console.healthcheck.nonredis.redisconfig;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractCrossDcIntervalCheck;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterServiceImpl;
import com.ctrip.xpipe.redis.console.service.impl.DcServiceImpl;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class RedisConfigCheckMonitor extends AbstractCrossDcIntervalCheck {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private DcServiceImpl dcService;

    @Autowired
    private DcClusterServiceImpl dcClusterService;

    private static final String ACTIVE_DEFAULT_REDIS_CHECK_RULE = "active.default.redis.check.rule";

    @Override
    protected void doCheck() {
        if(consoleConfig.isRedisConfigCheckMonitorOpen()) {
            String redisConfigCheckRule = consoleConfig.getRedisConfigCheckRules();
            if(StringUtil.isEmpty(redisConfigCheckRule)) {
                logger.info("[RedisConfigCheckMonitor][doCheck] no ready to add redis config check rule");
                return;
            }

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
    }

    private void addRedisConfigCheckRulesToDCCluster(ClusterMeta clustMeta, DcMeta dcMeta, String readyToAddRedisConfigRule) {
        DcClusterCreateInfo dcClusterCreateInfo = dcClusterService.findDcClusterCreateInfo(dcMeta.getId(), clustMeta.getId());
        String oldRedisConfigCheckRule = dcClusterCreateInfo.getRedisCheckRule();
        String newRedisConfigCheckRule = generateNewRedisConfigCheckRule(oldRedisConfigCheckRule, readyToAddRedisConfigRule);

        if(!newRedisConfigCheckRule.equals(oldRedisConfigCheckRule)) {
            dcClusterCreateInfo.setRedisCheckRule(newRedisConfigCheckRule);
            dcClusterService.updateDcCluster(dcClusterCreateInfo);
            CatEventMonitor.DEFAULT.logEvent(ACTIVE_DEFAULT_REDIS_CHECK_RULE, String.format("redis check rule of dc:%s cluster:%s was changed from %s to %s",
                    dcMeta.getId(), clustMeta.getId(), oldRedisConfigCheckRule, newRedisConfigCheckRule));
        }
    }

    @VisibleForTesting
    protected String generateNewRedisConfigCheckRule(String oldRedisConfigRule, String readyToAddRedisConfigRule) {
        if(StringUtil.isEmpty(oldRedisConfigRule))
            return readyToAddRedisConfigRule;

        List<String> newRules = Stream.of(Arrays.asList(oldRedisConfigRule.split(",")), Arrays.asList(readyToAddRedisConfigRule.split(",")))
                .flatMap(Collection::stream).distinct().collect(Collectors.toList());

        return StringUtil.join(",", (arg)->(arg), newRules);
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
