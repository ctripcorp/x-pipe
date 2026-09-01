package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.redis.checker.healthcheck.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.resource.Resource.PING_DELAY_INFO_EXECUTORS;
import static com.ctrip.xpipe.redis.checker.resource.Resource.PING_DELAY_INFO_SCHEDULED;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 3:33 PM
 */
@Component
public class InfoActionFactory implements RedisHealthCheckActionFactory<InfoAction>, OneWaySupport, BiDirectionSupport, SingleDcSupport, LocalDcSupport {

    @Resource(name = PING_DELAY_INFO_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = PING_DELAY_INFO_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private List<InfoActionListener> listeners;

    @Autowired
    private List<InfoActionController> controllers;

    @Override
    public InfoAction create(RedisHealthCheckInstance instance) {
        InfoAction action = new InfoAction(scheduled, instance, executors);
        action.addListeners(listeners);
        action.addControllers(controllers);
        return action;
    }
}
