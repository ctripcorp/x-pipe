package com.ctrip.xpipe.redis.console.healthcheck.actions.ping;


import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DelayPingActionCollector;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
@Component
public class PingActionFactory implements HealthCheckActionFactory<PingAction> {

    @Resource(name = ConsoleContextConfig.PING_DELAY_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.PING_DELAY_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private List<PingActionListener> listeners;

    @Autowired
    private DelayPingActionCollector collector;

    @Override
    public PingAction create(RedisHealthCheckInstance instance) {
        PingAction pingAction = new PingAction(scheduled, instance, executors);
        pingAction.addListeners(listeners);
        if(instance instanceof DefaultRedisHealthCheckInstance) {
            pingAction.addListener(((DefaultRedisHealthCheckInstance)instance).createPingListener());
        }
        pingAction.addListener(collector.createPingActionListener());
        return pingAction;
    }
}
