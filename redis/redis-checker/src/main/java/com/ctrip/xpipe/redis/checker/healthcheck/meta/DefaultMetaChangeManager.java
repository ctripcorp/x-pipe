package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
@Component
public class DefaultMetaChangeManager implements MetaChangeManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMetaChangeManager.class);

    @Autowired
    private HealthCheckInstanceManager instanceManager;
    
    @Autowired
    private HealthCheckEndpointFactory healthCheckEndpointFactory;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    private MetaCache metaCache;

    private ScheduledFuture future;

    private ConcurrentMap<String, DcMetaChangeManager> dcMetaChangeManagers = Maps.newConcurrentMap();

    @Override
    public void start() {
        int interval = Math.max(checkerConfig.getRedisReplicationHealthCheckInterval(),
                checkerConfig.getCheckerMetaRefreshIntervalMilli());
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                checkDcMetaChange();
                instanceManager.checkInstancesMiss(metaCache.getXpipeMeta());
            }
        }, interval * 2, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if(future != null) {
            future.cancel(true);
        }
    }

    private void checkDcMetaChange() {
        XpipeMeta meta = metaCache.getXpipeMeta();
        for(Map.Entry<String, DcMeta> entry : meta.getDcs().entrySet()) {
            String dcId = entry.getKey();
            if(checkerConfig.getIgnoredHealthCheckDc().contains(dcId)) {
                ignore(dcId);
                continue;
            }
            getOrCreate(dcId).compare(entry.getValue());
        }
    }

    @Override
    public DcMetaChangeManager getOrCreate(String dcId) {
        return MapUtils.getOrCreate(dcMetaChangeManagers, dcId, new ObjectFactory<DcMetaChangeManager>() {
                    @Override
                    public DcMetaChangeManager create() {
                        return new DefaultDcMetaChangeManager(dcId, instanceManager, healthCheckEndpointFactory, metaCache);
                    }
                });
    }

    @Override
    public void ignore(String dcId) {
        if(!dcMetaChangeManagers.containsKey(dcId)) {
            logger.warn("[ignore] not found dcId: {}", dcId);
            return;
        }
        try {
            dcMetaChangeManagers.get(dcId).stop();
        } catch (Exception e) {
            logger.error("[ignore]", e);
        }
    }

    @Override
    public void startIfPossible(String dcId) {
        logger.info("[startIfPossible] dcId: {}", dcId);
        if(metaCache.getXpipeMeta().findDc(dcId) == null) {
            logger.info("[startIfPossible] not found dcId: {}", dcId);
            return;
        }
        try {
            DcMetaChangeManager manager = getOrCreate(dcId);
            manager.compare(metaCache.getXpipeMeta().findDc(dcId));
            manager.start();
        } catch (Exception e) {
            logger.error("[start]", e);
        }
    }
}
