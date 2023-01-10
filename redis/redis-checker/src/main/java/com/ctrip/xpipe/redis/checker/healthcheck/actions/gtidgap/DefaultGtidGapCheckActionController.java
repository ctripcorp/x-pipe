package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
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
        return super.shouldCheck(instance) && !DcGroupType.isNullOrDrMaster(instance.getCheckInfo().getDcGroupType());
    }

}
