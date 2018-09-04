package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.delay.DefaultDelayContext;
import com.ctrip.xpipe.redis.console.healthcheck.delay.DelayCollector;
import com.ctrip.xpipe.redis.console.healthcheck.delay.DelayContext;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultHealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.ping.DefaultPingContext;
import com.ctrip.xpipe.redis.console.healthcheck.ping.PingContext;
import com.ctrip.xpipe.redis.console.healthcheck.redis.DefaultRedisContext;
import com.ctrip.xpipe.redis.console.healthcheck.redis.RedisContext;
import com.ctrip.xpipe.redis.console.healthcheck.redis.conf.DefaultRedisConfContext;
import com.ctrip.xpipe.redis.console.healthcheck.redis.conf.RedisConfContext;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
@Component
public class DefaultHealthCheckContextFactory implements HealthCheckContextFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckContextFactory.class);

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Autowired
    private List<DelayCollector> collectors;

    @Override
    public HealthCheckContext create(RedisHealthCheckInstance instance, RedisMeta redis) {
        RedisContext redisContext = new DefaultRedisContext(scheduled, instance);
        if(redis.isMaster()) {
            ((DefaultRedisContext) redisContext).setRedisInfo(new MasterInfo());
        }
        RedisConfContext redisConfContext = new DefaultRedisConfContext(scheduled, instance);
        PingContext pingContext = new DefaultPingContext(scheduled, instance);
        DelayContext delayContext = new DefaultDelayContext(scheduled, instance, executors, collectors);
        return new DefaultHealthCheckContext(redisContext, redisConfContext, delayContext, pingContext);
    }
}
