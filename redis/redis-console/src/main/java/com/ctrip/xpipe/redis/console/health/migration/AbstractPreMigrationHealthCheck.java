package com.ctrip.xpipe.redis.console.health.migration;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.AbstractIntervalCheck;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.RedisSessionManager;
import com.ctrip.xpipe.redis.console.health.SampleMonitor;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import rx.Observable;
import rx.Subscriber;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Sep 21, 2017
 */
public abstract class AbstractPreMigrationHealthCheck
        implements MigrationHealthCheck {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected MetaCache metaCache;

    @Autowired
    protected AlertManager alertManager;

    @Autowired
    protected RedisSessionManager redisSessionManager;

    @Override
    public void check() {

    }

    protected long getIntervalMilli(){
        return consoleConfig.getRedisMigrationHealthCheckInterval();
    }

    @Override
    public void fail(Throwable throwable) {

    }
}
