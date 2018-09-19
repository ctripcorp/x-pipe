package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.redis.console.healthcheck.action.HealthStatus.PING_DOWN_AFTER_MILLI;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
@Component
public class DefaultSiteReliabilityChecker implements SiteReliabilityChecker {

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private MetaCache metaCache;

    private Map<String, AtomicInteger> dcMarkDownCounter = Maps.newConcurrentMap();

    private Map<String, CommandFuture<Boolean>> dcMarkDownChecker = Maps.newConcurrentMap();

    @Override
    public CommandFuture<Boolean> check(AbstractInstanceEvent event) {
        RedisInstanceInfo info = event.getInstance().getRedisInstanceInfo();
        AtomicInteger counter = dcMarkDownCounter.get(info.getDcId());
        if(counter == null) {
            synchronized (this) {
                if(dcMarkDownCounter.get(info.getDcId()) == null) {
                    dcMarkDownCounter.putIfAbsent(info.getDcId(), new AtomicInteger(0));
                    dcMarkDownChecker.put(info.getDcId(), new SiteReliabilityCommand(info.getDcId()).execute());
                }
            }
        }
        counter = dcMarkDownCounter.get(info.getDcId());
        counter.incrementAndGet();
        return dcMarkDownChecker.get(info.getDcId());
    }

    protected int getCheckInterval() {
        return PING_DOWN_AFTER_MILLI / 5;
    }

    @VisibleForTesting
    public DefaultSiteReliabilityChecker setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }

    @VisibleForTesting
    public DefaultSiteReliabilityChecker setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    class SiteReliabilityCommand extends AbstractCommand<Boolean> {

        private String dcId;

        public SiteReliabilityCommand(String dcId) {
            this.dcId = dcId;
        }

        @Override
        protected void doExecute() throws Exception {
            scheduled.schedule(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    boolean result = siteReliable();
                    logger.info("[SiteReliabilityCommand][SiteReliable or Not] {}: {}", dcId, result);
                    future().setSuccess(result);
                    synchronized (DefaultSiteReliabilityChecker.this) {
                        dcMarkDownChecker.remove(dcId);
                        dcMarkDownCounter.remove(dcId);
                    }
                }
            }, getCheckInterval(), TimeUnit.MILLISECONDS);
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        private boolean siteReliable() {
            int totalRedis = metaCache.getRedisNumOfDc(dcId);
            int downRedis = dcMarkDownCounter.get(dcId).get();
            return downRedis < totalRedis/2;
        }
    }


}
