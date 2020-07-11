package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.stereotype.Component;

@Component
public class MultiMasterDelayActionController extends CurrentDcCheckController implements DelayActionController, BiDirectionSupport {

    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return super.shouldCheck(instance) || instance.getRedisInstanceInfo().isMaster();
    }

}
