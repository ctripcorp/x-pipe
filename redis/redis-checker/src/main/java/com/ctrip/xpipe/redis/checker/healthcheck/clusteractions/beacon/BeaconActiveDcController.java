package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/1/17
 */
@Component
public class BeaconActiveDcController implements BeaconMetaController {

    private static Logger logger = LoggerFactory.getLogger(BeaconActiveDcController.class);

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    private ConsoleCommonConfig config;

    private MetaCache metaCache;

    @Autowired
    public BeaconActiveDcController(ConsoleCommonConfig consoleCommonConfig, MetaCache metaCache) {
        this.config = consoleCommonConfig;
        this.metaCache = metaCache;
    }

    @Override
    public boolean shouldCheck(ClusterHealthCheckInstance instance) {
        ClusterInstanceInfo info = instance.getCheckInfo();
        if (!info.getClusterType().supportMigration()) {
            logger.debug("[shouldCheck][{}][skip] {} unsupport", info.getClusterId(), info.getClusterType());
        }
        if(!CURRENT_DC.equalsIgnoreCase(info.getActiveDc())) {
            logger.debug("[shouldCheck][{}][skip] active dc {}", info.getClusterId(), info.getActiveDc());
            return false;
        }
        if (!StringUtil.isEmpty(config.getBeaconSupportZone()) && !metaCache.isDcInRegion(info.getActiveDc(), config.getBeaconSupportZone())) {
            logger.debug("[shouldCheck][{}] active dc {} not in {}", info.getClusterId(), info.getActiveDc(), config.getBeaconSupportZone());
            return false;
        }

        return true;
    }

}
