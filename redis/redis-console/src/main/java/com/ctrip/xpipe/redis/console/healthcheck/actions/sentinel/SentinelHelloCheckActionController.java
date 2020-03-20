package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.console.healthcheck.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class SentinelHelloCheckActionController implements HealthCheckActionController, SentinelHelloCollector {

    @Autowired
    private SentinelCheckControllerManager checkControllerManager;

    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        return checkControllerManager.getCheckController(info.getClusterId(), info.getShardId()).shouldCheck(instance);
    }

    public void onAction(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        checkControllerManager.getCheckController(info.getClusterId(), info.getShardId()).onAction(context);
    }

    public void stopWatch(HealthCheckAction action) {

    }

}
