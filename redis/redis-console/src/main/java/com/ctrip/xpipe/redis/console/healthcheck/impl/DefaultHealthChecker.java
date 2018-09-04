package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.console.healthcheck.meta.MetaChangeManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.entity.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
@ConditionalOnProperty(name = { com.ctrip.xpipe.redis.console.health.HealthChecker.ENABLED }, matchIfMissing = true)
public class DefaultHealthChecker extends AbstractLifecycle implements HealthChecker {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Autowired
    private MetaChangeManager metaChangeManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @PostConstruct
    public void postConstruct() {
        try {
            LifecycleHelper.initializeIfPossible(this);
        } catch (Exception e) {
            logger.error("[initialize]", e);
        }
        try {
            LifecycleHelper.startIfPossible(this);
        } catch (Exception e) {
            logger.error("[start]", e);
        }

    }

    @PreDestroy
    public void preDestroy() {
        try {
            LifecycleHelper.stopIfPossible(this);
        } catch (Exception e) {
            logger.error("[start]", e);
        }
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        generateHealthCheckInstances();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        instanceManager.start();
        metaChangeManager.start();
    }

    @Override
    protected void doStop() throws Exception {
        instanceManager.stop();
        metaChangeManager.stop();
        super.doStop();
    }


    private void generateHealthCheckInstances() {
        generateHealthCheckInstances(0);
    }

    private void generateHealthCheckInstances(int attempt) {
        int BACKOFF_CAP = 12;
        int interval = Math.max(200, 2 << attempt);
        XpipeMeta meta = metaCache.getXpipeMeta();
        if(meta == null) {
            scheduled.schedule(new Runnable() {
                @Override
                public void run() {
                    generateHealthCheckInstances(Math.min(BACKOFF_CAP, attempt + 1));
                }
            }, interval, TimeUnit.MILLISECONDS);
            return;
        }
        for(DcMeta dcMeta : meta.getDcs().values()) {
            if(consoleConfig.getIgnoredHealthCheckDc().contains(dcMeta.getId())) {
                continue;
            }
            for(ClusterMeta cluster : dcMeta.getClusters().values()) {
                for(ShardMeta shard : cluster.getShards().values()) {
                    for(RedisMeta redis : shard.getRedises()) {
                        instanceManager.getOrCreate(redis);
                    }
                }
            }
        }
    }

}
