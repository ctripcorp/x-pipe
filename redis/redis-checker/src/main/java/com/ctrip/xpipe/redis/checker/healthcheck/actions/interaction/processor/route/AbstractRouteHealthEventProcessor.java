package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceLongDelay;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.HealthEventProcessor;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * @author Slight
 * <p>
 * Nov 25, 2021 1:05 AM
 */
public abstract class AbstractRouteHealthEventProcessor implements HealthEventProcessor {

    private final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @VisibleForTesting
    @Autowired
    public ProxyManager proxyManager;

    @VisibleForTesting
    @Resource(name = SCHEDULED_EXECUTOR)
    public ScheduledExecutorService scheduled;

    @Autowired(required = false)
    private GroupCheckerLeaderElector clusterServer;

    private final Set<Pair<String, String>> cache = Sets.newConcurrentHashSet();

    private boolean tryDedupe(AbstractInstanceEvent event) {
        Pair<String, String> id = identifierOfEvent(event);
        if (cache.add(id)) {
            scheduled.schedule(() -> {
                cache.remove(id);
            }, getHoldingMillis(), TimeUnit.MILLISECONDS);
            return false;
        }
        return true;
    }

    @VisibleForTesting
    public void undoDedupe(AbstractInstanceEvent event) {
        cache.remove(identifierOfEvent(event));
    }

    @Override
    public void onEvent(AbstractInstanceEvent event) {
        if (clusterServer != null && !clusterServer.amILeader()) {
            return;
        }
        if (currentDcId.equals(event.getInstance().getCheckInfo().getDcId())) {
            return;
        }
        if (supportEvent(event)) {
            if (tryDedupe(event)) return;
            doOnEvent(event);
        }
    }

    @VisibleForTesting
    protected void doOnEvent(AbstractInstanceEvent instanceSick) {
        ProxyTunnelInfo proxyTunnelInfo = findProxyTunnelInfo(instanceSick);
        if (proxyTunnelInfo == null) {
            logger.warn("[doOnEvent]proxy chain not found for {}", instanceSick);
            return;
        }
        long expected = isProbablyHealthyInXSeconds(instanceSick);
        logger.info("[doOnEvent]instance({}) is probably healthy in {} seconds", instanceSick, expected);
        if (0 >= expected) {
            tryRecover(instanceSick, proxyTunnelInfo);
        } else {
            scheduled.schedule(()->undoDedupe(instanceSick), expected, TimeUnit.SECONDS);
        }
    }

    protected abstract ProxyTunnelInfo findProxyTunnelInfo(AbstractInstanceEvent instanceSick);

    protected abstract long isProbablyHealthyInXSeconds(AbstractInstanceEvent instanceSick);

    protected void tryRecover(AbstractInstanceEvent instanceSick, ProxyTunnelInfo proxyTunnelInfo) {
        logEvent(instanceSick);
        proxyManager.closeProxyTunnel(proxyTunnelInfo);
    }

    protected boolean supportEvent(AbstractInstanceEvent event) {
        return event instanceof InstanceLongDelay;
    }

    protected Pair<String, String> identifierOfEvent(AbstractInstanceEvent event) {
        RedisInstanceInfo info = event.getInstance().getCheckInfo();
        return Pair.from(info.getDcId(), info.getShardId());
    }

    protected void logEvent(AbstractInstanceEvent instanceSick) {
        RedisHealthCheckInstance instance = instanceSick.getInstance();
        EventMonitor.DEFAULT.logEvent("XPIPE.PROXY.CHAIN", String.format("[CLOSE]%s: %s",
                instance.getCheckInfo().getDcId(), instance.getCheckInfo().getShardId()));
    }

    protected long getHoldingMillis() {
        return 60 * 1000;
    }
}
