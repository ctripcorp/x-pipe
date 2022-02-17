package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.redis.checker.healthcheck.CrossDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

@Component
public class CrossDcSentinelCheckController implements SentinelActionController, CrossDcSupport {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        logger.info("[{}][shouldCheck]{}", LOG_TITLE, instance.toString());
        return !instance.getCheckInfo().isMaster();
    }

}
