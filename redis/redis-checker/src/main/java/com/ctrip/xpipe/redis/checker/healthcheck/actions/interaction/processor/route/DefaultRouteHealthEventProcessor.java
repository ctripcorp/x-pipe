package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
public class DefaultRouteHealthEventProcessor extends AbstractRouteHealthEventProcessor implements OneWaySupport {

    @VisibleForTesting
    @Autowired(required = false)
    public MetaCache metaCache;

    @VisibleForTesting
    @Autowired
    public RedisSessionManager redisSessionManager;

    @Override
    protected ProxyTunnelInfo findProxyTunnelInfo(AbstractInstanceEvent instanceSick) {
        RedisInstanceInfo instanceInfo = instanceSick.getInstance().getCheckInfo();
        return proxyManager.getProxyTunnelInfo(instanceInfo.getDcId(),
                instanceInfo.getClusterId(), instanceInfo.getShardId(), "UNSET");
    }

    @Override
    protected long isProbablyHealthyInXSeconds(AbstractInstanceEvent instanceSick) {
        try {
            RedisHealthCheckInstance instance = instanceSick.getInstance();
            if (!isRedisInFullSync(instance)) return 0;
            String clusterId = instance.getCheckInfo().getClusterId();
            String shardId = instance.getCheckInfo().getShardId();
            long dataSize = getMasterDataSize(clusterId, shardId);
            return getDelaySeconds(dataSize);
        } catch (Exception e) {
            logger.error("[isProbablyHealthyInXSeconds]", e);
            return 30;
        }
    }

    private long getMasterDataSize(String clusterId, String shardId) throws Exception {
        HostPort hostPort = metaCache.findMaster(clusterId, shardId);
        RedisSession redisSession = redisSessionManager.findOrCreateSession(hostPort);
        InfoResultExtractor masterInfo = redisSession.syncInfo(InfoCommand.INFO_TYPE.ALL);

        long dataSize = masterInfo.getUsedMemory();
        if (masterInfo.isROR()) {
            dataSize += masterInfo.getSwapUsedDbSize();
        }

        return dataSize;
    }

    @VisibleForTesting
    protected boolean isRedisInFullSync(RedisHealthCheckInstance instance) throws InterruptedException, ExecutionException, TimeoutException {
        InfoResultExtractor info = instance.getRedisSession().syncInfo(InfoCommand.INFO_TYPE.REPLICATION);
        return info.extractAsInteger("master_sync_in_progress") == 1;
    }

    @VisibleForTesting
    protected long getDelaySeconds(long dataSize) {
        long base = 3 * 60; // 3 minutes / per GB size
        long gb = 1024 * 1024 * 1024;
        return base * (dataSize/gb + 1);
    }
}
