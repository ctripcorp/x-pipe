package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DelayPingActionCollector;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingService;
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
public class DelayActionFactory implements HealthCheckActionFactory<DelayAction> {

    @Autowired
    private PingService pingService;

    @Resource(name = ConsoleContextConfig.PING_DELAY_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.PING_DELAY_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private List<DelayActionListener> listeners;

    @Autowired
    private DelayPingActionCollector collector;

    @Override
    public DelayAction create(RedisHealthCheckInstance instance) {
        DelayAction delayAction = new DelayAction(scheduled, instance, executors, pingService);
        delayAction.addListeners(listeners);
        if(instance instanceof DefaultRedisHealthCheckInstance) {
            delayAction.addListener(((DefaultRedisHealthCheckInstance)instance).createDelayListener());
        }
        delayAction.addListener(collector.createDelayActionListener());
        return delayAction;
    }
}
