package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/1/17
 */
@Component
public class BeaconActiveDcController implements BeaconMetaController {

    private static Logger logger = LoggerFactory.getLogger(BeaconActiveDcController.class);

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    @Override
    public boolean shouldCheck(ClusterHealthCheckInstance instance) {
        ClusterInstanceInfo info = instance.getCheckInfo();
        if(!CURRENT_DC.equalsIgnoreCase(info.getActiveDc())) {
            logger.debug("[shouldCheck] not check in backup dc: {}", info);
            return false;
        }

        return true;
    }

}
