package com.ctrip.xpipe.redis.console.healthcheck.nonredis.redisconfig;

import com.ctrip.xpipe.cluster.ClusterType;
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
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
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

    @Override
    protected void doCheck() {
        if(consoleConfig.isRedisConfigCheckMonitorOpen()) {
            String redisConfigCheckRule = consoleConfig.getRedisConfigCheckRules();
            if(null == redisConfigCheckRule || redisConfigCheckRule.equals("")) {
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
        String newRedisConfigCheckRule = generateNewRedisConfigRule(dcClusterCreateInfo.getRedisConfigRule(), readyToAddRedisConfigRule);

        dcClusterCreateInfo.setRedisConfigRule(newRedisConfigCheckRule);
        logger.info("[addRedisConfigCheckRulesToDCCluster] dc:{} cluster:{} from {} to {}", dcMeta.getId(), clustMeta.getId(), dcClusterCreateInfo.getRedisConfigRule(), newRedisConfigCheckRule);
        dcClusterService.updateDcCluster(dcClusterCreateInfo);
    }

    @VisibleForTesting
    protected String generateNewRedisConfigRule(String oldRedisConfigRule, String readyToAddRedisConfigRule) {
        if(oldRedisConfigRule == null || "".equals(oldRedisConfigRule))
            return readyToAddRedisConfigRule;

        List<String> newRules = Stream.of(Arrays.asList(oldRedisConfigRule.split(",")), Arrays.asList(readyToAddRedisConfigRule.split(",")))
                .flatMap(Collection::stream).distinct().collect(Collectors.toList());

        StringBuilder stringBuilder = new StringBuilder();
        newRules.forEach(s -> stringBuilder.append(s).append(","));
        stringBuilder.deleteCharAt(stringBuilder.length() -1);
        return stringBuilder.toString();
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
