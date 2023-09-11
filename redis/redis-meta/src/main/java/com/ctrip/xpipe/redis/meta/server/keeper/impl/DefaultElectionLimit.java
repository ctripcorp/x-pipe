package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.keeper.ElectionLimit;
import com.ctrip.xpipe.utils.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * 2023/9/7
 * @author yu
 */

@Component
public class DefaultElectionLimit implements ElectionLimit {

    @Autowired
    private MetaServerConfig config;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> scheduledFuture;

    private Map<String, AtomicInteger> shardKeeperElectCount;

    public DefaultElectionLimit() {
    }

    @PostConstruct
    public void init() {
        shardKeeperElectCount = new ConcurrentHashMap<>();
        scheduledFuture = scheduled.scheduleAtFixedRate(() -> shardKeeperElectCount.clear(), config.getKeeperElectTimingCycleMills(),
                config.getKeeperElectTimingCycleMills(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void preDestroy() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    @Override
    public boolean tryAcquire(String shardKey) {
        AtomicInteger alreadyKeeperElectCount = MapUtils.getOrCreate(shardKeeperElectCount, shardKey, () -> new AtomicInteger(0));
        return alreadyKeeperElectCount.getAndIncrement() < config.getMaxKeeperElectTimesInFixedTime();
    }
}
