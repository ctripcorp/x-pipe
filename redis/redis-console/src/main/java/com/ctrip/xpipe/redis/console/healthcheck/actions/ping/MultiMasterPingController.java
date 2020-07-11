package com.ctrip.xpipe.redis.console.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.stereotype.Component;

@Component
public class MultiMasterPingController extends CurrentDcCheckController implements PingActionController, BiDirectionSupport {

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return super.shouldCheck(instance) || instance.getRedisInstanceInfo().isMaster();
    }

}
