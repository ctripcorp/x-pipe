package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceHalfSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Try to disconnect proxy chain if the chain is blocked for a long time
 * Check if the instance is through proxy connected, and if it is sick
 *
 * up: we don't care
 * down: ping not response, is not about the proxy chain, is about the redis, that it's not working or networking is not working*/
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class RouteHealthEventProcessor implements HealthEventProcessor {

    private Logger logger = LoggerFactory.getLogger(RouteHealthEventProcessor.class);

    @Autowired(required = false)
    private MetaCache metaCache;

    @Autowired
    private RedisSessionManager redisSessionManager;

    @Autowired
    private ProxyService proxyService;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired(required = false)
    private CrossDcClusterServer crossDcClusterServer;

    @Override
    public void onEvent(AbstractInstanceEvent event) {
        // make sure only one execute the disturb
        if (crossDcClusterServer != null && !crossDcClusterServer.amILeader()) {
            return;
        }
        //only deal with sick instance
        if (event instanceof InstanceHalfSick) {
            doOnEvent((InstanceHalfSick) event);
        }
    }

    @VisibleForTesting
    protected void doOnEvent(InstanceHalfSick instanceSick) {
        RedisInstanceInfo instanceInfo = instanceSick.getInstance().getRedisInstanceInfo();
        ProxyChain proxyChain = proxyService.getProxyChain(instanceInfo.getDcId(),
                instanceInfo.getClusterId(), instanceInfo.getShardId());
        if (proxyChain == null) {
            return;
        }
        try {
            InfoResultExtractor info = instanceSick.getInstance().getRedisSession()
                    .syncInfo(InfoCommand.INFO_TYPE.REPLICATION);
            if (isRedisInFullSync(info)) {
                new FullSyncHandler(info, instanceSick.getInstance(), proxyChain).handle();
            } else {
                new PartialSyncHandler(info, instanceSick.getInstance(), proxyChain).handle();
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

        protected ProxyChain proxyChain;

        public ProxyChainBlockedHandler(InfoResultExtractor info, RedisHealthCheckInstance instance, ProxyChain proxyChain) {
            this.info = info;
            this.instance = instance;
            this.proxyChain = proxyChain;
        }

        protected abstract void handle();
    }

    @VisibleForTesting
    protected class FullSyncHandler extends ProxyChainBlockedHandler {

        public FullSyncHandler(InfoResultExtractor info, RedisHealthCheckInstance instance, ProxyChain proxyChain) {
            super(info, instance, proxyChain);
        }

        @Override
        public void handle() {
            if (bgSaveOverDue()) {
                closeProxyChain(instance, proxyChain);
            }
        }

        private boolean bgSaveOverDue() {
            String clusterId = instance.getRedisInstanceInfo().getClusterId();
            String shardId = instance.getRedisInstanceInfo().getShardId();
            try {
                HostPort hostPort = metaCache.findMaster(clusterId, shardId);
                RedisSession redisSession = redisSessionManager.findOrCreateSession(hostPort);
                InfoResultExtractor masterInfo = redisSession.syncInfo(InfoCommand.INFO_TYPE.PERSISTENCE);
                long rdbSize = masterInfo.extractAsLong("rdb_last_cow_size");
                long rdbExpectedRemainTime = getDelaySeconds(rdbSize)
                        - TimeUnit.MILLISECONDS.toSeconds(instance.getHealthCheckConfig().delayDownAfterMilli())/2;

                if (rdbExpectedRemainTime > 0) {
                    scheduled.schedule(new DelayedCloseTask(proxyChain, instance),
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

        public PartialSyncHandler(InfoResultExtractor info, RedisHealthCheckInstance instance, ProxyChain proxyChain) {
            super(info, instance, proxyChain);
        }

        @Override
        public void handle() {
            closeProxyChain(instance, proxyChain);
        }
    }

    protected void closeProxyChain(RedisHealthCheckInstance instance, ProxyChain proxyChain) {
        EventMonitor.DEFAULT.logEvent("XPIPE.PROXY.CHAIN", String.format("[CLOSE]%s: %s",
                instance.getRedisInstanceInfo().getDcId(), instance.getRedisInstanceInfo().getShardId()));

        List<TunnelInfo> tunnelInfos = proxyChain.getTunnels();
        List<HostPort> backends = Lists.newArrayList();
        for (TunnelInfo tunnelInfo : tunnelInfos) {
            HostPort hostPort = tunnelInfo.getTunnelStatsResult().getBackend();
            backends.add(new HostPort(tunnelInfo.getProxyModel().getHostPort().getHost(), hostPort.getPort()));
        }
        proxyService.deleteProxyChain(backends);
    }

    private class DelayedCloseTask implements Runnable {

        private ProxyChain proxyChain;

        private RedisHealthCheckInstance instance;

        public DelayedCloseTask(ProxyChain proxyChain, RedisHealthCheckInstance instance) {
            this.proxyChain = proxyChain;
            this.instance = instance;
        }

        @Override
        public void run() {
            if (redisStillInFullSync()) {
                closeProxyChain(instance, proxyChain);
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
    public RouteHealthEventProcessor setProxyService(ProxyService proxyService) {
        this.proxyService = proxyService;
        return this;
    }

    @VisibleForTesting
    public RouteHealthEventProcessor setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }
}
