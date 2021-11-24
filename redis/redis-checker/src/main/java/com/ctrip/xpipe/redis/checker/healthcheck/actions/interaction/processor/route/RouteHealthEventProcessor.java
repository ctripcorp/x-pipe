package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.HealthEventProcessor;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceHalfSick;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

/**
 * Try to disconnect proxy chain if the chain is blocked for a long time
 * Check if the instance is through proxy connected, and if it is sick
 *
 * up: we don't care
 * down: ping not response, is not about the proxy chain, is about the redis, that it's not working or networking is not working
 * sick: this should not be expected
 * half-sick: we take over at this step, to prevent a predictable data lack*/
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class RouteHealthEventProcessor implements HealthEventProcessor {

    private Logger logger = LoggerFactory.getLogger(RouteHealthEventProcessor.class);

    @Autowired(required = false)
    private MetaCache metaCache;

    @Autowired
    private RedisSessionManager redisSessionManager;

    @Autowired
    private ProxyManager proxyManager;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired(required = false)
    private GroupCheckerLeaderElector clusterServer;

    private Set<Pair<String, String>> events = Sets.newConcurrentHashSet();

    @Override
    public void onEvent(AbstractInstanceEvent event) {
        // make sure only one execute the disturb
        if (clusterServer != null && !clusterServer.amILeader()) {
            return;
        }
        //only deal with sick instance
        if (event instanceof InstanceHalfSick) {
            RedisInstanceInfo info = event.getInstance().getCheckInfo();
            Pair<String, String> pair = Pair.from(info.getDcId(), info.getShardId());
            // duplicate reduction
            if (events.add(pair)) {
                scheduled.schedule(new Runnable() {
                    @Override
                    public void run() {
                        events.remove(pair);
                    }
                }, getHoldingMillis(), TimeUnit.MILLISECONDS);
                doOnEvent((InstanceHalfSick) event);
            }

        }
    }

    protected long getHoldingMillis() {
        return 60 * 1000;
    }

    @VisibleForTesting
    protected void doOnEvent(InstanceHalfSick instanceSick) {
        RedisInstanceInfo instanceInfo = instanceSick.getInstance().getCheckInfo();
        ProxyTunnelInfo proxyTunnelInfo = proxyManager.getProxyTunnelInfo(instanceInfo.getDcId(),
                instanceInfo.getClusterId(), instanceInfo.getShardId(), "UNSET");
        if (proxyTunnelInfo == null) {
            logger.warn("[doOnEvent]proxy chain not found for {}", instanceInfo);
            return;
        }
        try {
            InfoResultExtractor info = instanceSick.getInstance().getRedisSession()
                    .syncInfo(InfoCommand.INFO_TYPE.REPLICATION);
            if (isRedisInFullSync(info)) {
                logger.warn("[doOnEvent]in full sync {}", instanceInfo);
                new FullSyncHandler(info, instanceSick.getInstance(), proxyTunnelInfo).handle();
            } else {
                logger.info("[doOnEvent]in partial sync {}", instanceInfo);
                new PartialSyncHandler(info, instanceSick.getInstance(), proxyTunnelInfo).handle();
            }
        } catch (Exception e) {
            logger.error("[doOnEvent]", e);
        }
    }

    @VisibleForTesting
    protected boolean isRedisInFullSync(InfoResultExtractor info) {
        return info.extractAsInteger("master_sync_in_progress") == 1;
    }

    private abstract class ProxyChainBlockedHandler {

        protected InfoResultExtractor info;

        protected RedisHealthCheckInstance instance;

        protected ProxyTunnelInfo proxyTunnelInfo;

        public ProxyChainBlockedHandler(InfoResultExtractor info, RedisHealthCheckInstance instance, ProxyTunnelInfo proxyTunnelInfo) {
            this.info = info;
            this.instance = instance;
            this.proxyTunnelInfo = proxyTunnelInfo;
        }

        protected abstract void handle();
    }

    @VisibleForTesting
    protected class FullSyncHandler extends ProxyChainBlockedHandler {

        public FullSyncHandler(InfoResultExtractor info, RedisHealthCheckInstance instance, ProxyTunnelInfo proxyTunnelInfo) {
            super(info, instance, proxyTunnelInfo);
        }

        @Override
        public void handle() {
            if (bgSaveOverDue()) {
                closeProxyChain(instance, proxyTunnelInfo);
            }
        }

        private boolean bgSaveOverDue() {
            String clusterId = instance.getCheckInfo().getClusterId();
            String shardId = instance.getCheckInfo().getShardId();
            try {
                HostPort hostPort = metaCache.findMaster(clusterId, shardId);
                RedisSession redisSession = redisSessionManager.findOrCreateSession(hostPort);
                InfoResultExtractor masterInfo = redisSession.syncInfo(InfoCommand.INFO_TYPE.PERSISTENCE);
                long rdbSize = masterInfo.extractAsLong("rdb_last_cow_size");
                long rdbExpectedRemainTime = getDelaySeconds(rdbSize)
                        - TimeUnit.MILLISECONDS.toSeconds(instance.getHealthCheckConfig().delayDownAfterMilli())/2;

                if (rdbExpectedRemainTime > 0) {
                    scheduled.schedule(new DelayedCloseTask(proxyTunnelInfo, instance),
                            rdbExpectedRemainTime, TimeUnit.SECONDS);
                    return false;
                } else {
                    return true;
                }
            } catch (Exception e) {
                logger.error("[bgSaveOverDue]", e);
            }
            return false;
        }

    }

    @VisibleForTesting
    protected long getDelaySeconds(long rdbSize) {
        long base = 3 * 60; // 3 minutes / per GB size
        long gb = 1024 * 1024 * 1024;
        return base * (rdbSize/gb + 1);
    }

    @VisibleForTesting
    protected class PartialSyncHandler extends ProxyChainBlockedHandler {

        public PartialSyncHandler(InfoResultExtractor info, RedisHealthCheckInstance instance, ProxyTunnelInfo proxyTunnelInfo) {
            super(info, instance, proxyTunnelInfo);
        }

        @Override
        public void handle() {
            closeProxyChain(instance, proxyTunnelInfo);
        }
    }

    protected void closeProxyChain(RedisHealthCheckInstance instance, ProxyTunnelInfo proxyTunnelInfo) {
        EventMonitor.DEFAULT.logEvent("XPIPE.PROXY.CHAIN", String.format("[CLOSE]%s: %s",
                instance.getCheckInfo().getDcId(), instance.getCheckInfo().getShardId()));

        proxyManager.closeProxyTunnel(proxyTunnelInfo);
    }

    private class DelayedCloseTask implements Runnable {

        private ProxyTunnelInfo proxyTunnelInfo;

        private RedisHealthCheckInstance instance;

        public DelayedCloseTask(ProxyTunnelInfo proxyTunnelInfo, RedisHealthCheckInstance instance) {
            this.proxyTunnelInfo = proxyTunnelInfo;
            this.instance = instance;
        }

        @Override
        public void run() {
            if (redisStillInFullSync()) {
                closeProxyChain(instance, proxyTunnelInfo);
            }
        }

        private boolean redisStillInFullSync() {
            try {
                InfoResultExtractor info = instance.getRedisSession().syncInfo(InfoCommand.INFO_TYPE.REPLICATION);
                return isRedisInFullSync(info);
            } catch (Exception e) {
                logger.error("[redisStillInFullSync]", e);
            }
            return false;
        }
    }

    @VisibleForTesting
    public RouteHealthEventProcessor setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    @VisibleForTesting
    public RouteHealthEventProcessor setRedisSessionManager(RedisSessionManager redisSessionManager) {
        this.redisSessionManager = redisSessionManager;
        return this;
    }

    @VisibleForTesting
    public RouteHealthEventProcessor setProxyManager(ProxyManager proxyManager) {
        this.proxyManager = proxyManager;
        return this;
    }

    @VisibleForTesting
    public RouteHealthEventProcessor setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }
}
