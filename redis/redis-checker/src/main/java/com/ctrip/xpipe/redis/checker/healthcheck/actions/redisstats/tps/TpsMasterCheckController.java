package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TpsMasterCheckController extends CurrentDcCheckController implements TpsCheckController {

    @Autowired
    public TpsMasterCheckController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return super.shouldCheck(instance) && instance.getCheckInfo().isMaster();
    }

}
