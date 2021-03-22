package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.stereotype.Component;

@Component
public class MultiMasterPingController extends CurrentDcCheckController implements PingActionController, BiDirectionSupport {

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return super.shouldCheck(instance) || instance.getCheckInfo().isMaster();
    }

}
