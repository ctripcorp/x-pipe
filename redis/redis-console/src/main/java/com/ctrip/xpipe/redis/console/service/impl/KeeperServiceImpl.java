package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.KeeperService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Jan 23, 2018
 */
@Service
public class KeeperServiceImpl implements KeeperService {

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    ScheduledExecutorService scheduled;

    @Autowired
    MetaCache metaCache;

    @Autowired
    ConsoleConfig consoleConfig;

    private int refreshIntervalMilli = 2000;

    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    private Set<HostPort> keepers;

    @PostConstruct
    public void postConstruct() {

        logger.info("[postConstruct]{}", this);

        refreshIntervalMilli = consoleConfig.getCacheRefreshInterval();

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                keepers = metaCache.allKeepers();
            }
        }, refreshIntervalMilli, refreshIntervalMilli, TimeUnit.SECONDS);
    }

    @Override
    public boolean isKeeper(HostPort hostPort) {
        if(keepers == null) {
            keepers = metaCache.allKeepers();
        }
        return keepers.contains(hostPort);
    }
}
