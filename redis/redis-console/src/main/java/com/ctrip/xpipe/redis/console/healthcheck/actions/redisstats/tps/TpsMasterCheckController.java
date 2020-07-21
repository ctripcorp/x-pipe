package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.stereotype.Component;

@Component
public class TpsMasterCheckController extends CurrentDcCheckController implements TpsCheckController {

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return super.shouldCheck(instance) && instance.getRedisInstanceInfo().isMaster();
    }

}
