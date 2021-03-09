package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.stereotype.Component;

@Component
public class MultiMasterDelayActionController extends CurrentDcCheckController implements DelayActionController, BiDirectionSupport {

    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return super.shouldCheck(instance) || instance.getCheckInfo().isMaster();
    }

}
