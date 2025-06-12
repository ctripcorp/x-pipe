package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/17
 */
@Component
public class DefaultBeaconMetaController implements BeaconMetaController {

    private static Logger logger = LoggerFactory.getLogger(DefaultBeaconMetaController.class);

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    private ConsoleCommonConfig config;

    private MetaCache metaCache;

    @Autowired
    public DefaultBeaconMetaController(ConsoleCommonConfig consoleCommonConfig, MetaCache metaCache) {
        this.config = consoleCommonConfig;
        this.metaCache = metaCache;
    }

    @Override
    public boolean shouldCheck(ClusterHealthCheckInstance instance) {
        ClusterInstanceInfo info = instance.getCheckInfo();
        ClusterType clusterType = info.getClusterType();

        if (!BeaconSystem.anySupport(clusterType)) {
            logger.debug("[shouldCheck][{}][skip] {} unsupport", info.getClusterId(), info.getClusterType());
            return false;
        }
        Set<String> supportZones = config.getBeaconSupportZones();
        if (!supportZones.isEmpty() && supportZones.stream().noneMatch(zone -> metaCache.isDcInRegion(CURRENT_DC, zone))) {
            logger.debug("[shouldCheck][{}][skip] current {} not in {}", info.getClusterId(), CURRENT_DC, supportZones);
            return false;
        }
        if(clusterType.supportSingleActiveDC() && !CURRENT_DC.equalsIgnoreCase(info.getActiveDc())) {
            logger.debug("[shouldCheck][{}][skip] active dc {}", info.getClusterId(), info.getActiveDc());
            return false;
        }

        return true;
    }

}
