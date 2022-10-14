package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.SingleDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MasterTypePingController extends CurrentDcCheckController implements PingActionController, SingleDcSupport {

    @Autowired
    public MasterTypePingController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return !instance.getCheckInfo().getDcGroupType().isValue() && super.shouldCheck(instance);
    }

}
