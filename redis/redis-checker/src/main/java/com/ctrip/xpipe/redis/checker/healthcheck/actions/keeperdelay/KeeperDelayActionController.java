package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import org.springframework.stereotype.Component;

@Component
public class KeeperDelayActionController implements HealthCheckActionController<KeeperHealthCheckInstance> {

    @Override
    public boolean shouldCheck(KeeperHealthCheckInstance instance) {
        return instance.isTfs();
    }
}
