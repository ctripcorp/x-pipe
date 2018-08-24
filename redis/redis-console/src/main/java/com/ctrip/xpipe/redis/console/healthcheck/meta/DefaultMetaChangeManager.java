package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
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
public class DefaultMetaChangeManager extends AbstractLifecycle implements MetaChangeManager {

    @Autowired
    private HealthCheckInstanceManager instanceManager;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private MetaCache metaCache;

    private ScheduledFuture future;

    private ConcurrentMap<String, DcMetaChangeManager> dcMetaChangeManagers = Maps.newConcurrentMap();

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        int interval = consoleConfig.getRedisReplicationHealthCheckInterval();
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                checkDcMetaChange();
            }
        }, interval * 2, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        if(future != null) {
            future.cancel(true);
        }
        super.doStop();
    }

    private void checkDcMetaChange() {
        XpipeMeta meta = metaCache.getXpipeMeta();
        for(Map.Entry<String, DcMeta> entry : meta.getDcs().entrySet()) {
            String key = entry.getKey();
            if(consoleConfig.getIgnoredHealthCheckDc().contains(key)) {
                continue;
            }
            getOrCreate(key).compare(entry.getValue());
        }
    }

    private DcMetaChangeManager getOrCreate(String key) {
        return MapUtils.getOrCreate(dcMetaChangeManagers, key, new ObjectFactory<DcMetaChangeManager>() {
                    @Override
                    public DcMetaChangeManager create() {
                        return new DefaultDcMetaChangeManager(instanceManager);
                    }
                });
    }
}
