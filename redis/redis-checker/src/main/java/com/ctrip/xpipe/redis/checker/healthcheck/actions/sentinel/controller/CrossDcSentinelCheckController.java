package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.redis.checker.healthcheck.CrossDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import org.springframework.stereotype.Component;

@Component
public class CrossDcSentinelCheckController implements SentinelActionController, CrossDcSupport {

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
       return !instance.getCheckInfo().isMaster();
    }

}
