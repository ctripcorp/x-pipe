package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CRDTDelayActionController extends CurrentDcCheckController implements DelayActionController, BiDirectionSupport {
    
    @Autowired
    public CRDTDelayActionController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }

    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return super.shouldCheck(instance) || instance.getCheckInfo().isMaster();
    }

}
