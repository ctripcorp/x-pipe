package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class CurrentDcCheckController implements HealthCheckActionController<RedisHealthCheckInstance> {
    
    protected String currentDcId;
    
    public CurrentDcCheckController(String currentDcId) {
        this.currentDcId = currentDcId;
    }
    
    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        String dcId = instance.getCheckInfo().getDcId();
        return currentDcId.equalsIgnoreCase(dcId);
    }

}
