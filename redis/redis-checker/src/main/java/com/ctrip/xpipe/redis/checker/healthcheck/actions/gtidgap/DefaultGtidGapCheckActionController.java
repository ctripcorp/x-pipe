package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DefaultGtidGapCheckActionController extends CurrentDcCheckController implements GtidGapCheckActionController, OneWaySupport {

    @Autowired
    public DefaultGtidGapCheckActionController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        if (!super.shouldCheck(instance)) {
            return false;
        }
        String azGroupType = instance.getCheckInfo().getAzGroupType();
        if (StringUtil.isEmpty(azGroupType)) {
            return false;
        }
        return ClusterType.lookup(azGroupType) == ClusterType.SINGLE_DC;
    }

}
