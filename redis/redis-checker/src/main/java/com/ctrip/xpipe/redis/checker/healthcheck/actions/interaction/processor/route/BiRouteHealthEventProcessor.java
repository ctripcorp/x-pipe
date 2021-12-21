package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Slight
 * <p>
 * Nov 25, 2021 6:28 PM
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class BiRouteHealthEventProcessor extends AbstractRouteHealthEventProcessor implements BiDirectionSupport {

    @VisibleForTesting
    @Autowired
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
        RedisHealthCheckInstance instance = instanceSick.getInstance();
        String clusterId = instance.getCheckInfo().getClusterId();
        String shardId = instance.getCheckInfo().getShardId();
        try {
            HostPort master = metaCache.findMaster(clusterId, shardId);
            if (!isRedisInFullSyncTo(instance, master)) return 0;

            RedisSession redisSession = redisSessionManager.findOrCreateSession(master);
            InfoResultExtractor masterInfo = redisSession.syncCRDTInfo(InfoCommand.INFO_TYPE.PERSISTENCE);
            long rdbSize = masterInfo.extractAsLong("rdb_last_cow_size");
            return getDelaySeconds(rdbSize);
        } catch (Exception e) {
            return 30;
        }
    }

    @VisibleForTesting
    protected boolean isRedisInFullSyncTo(RedisHealthCheckInstance instance, HostPort master) throws InterruptedException, ExecutionException, TimeoutException {
        InfoResultExtractor info = instance.getRedisSession().syncCRDTInfo(InfoCommand.INFO_TYPE.REPLICATION);
        for (int i = 0; i < 16; i++) {
            if (master.getHost().equals(info.extract(String.format("peer%d_host", i)))
                    && master.getPort() == info.extractAsInteger(String.format("peer%d_port", i))) {
                return info.extractAsInteger(String.format("peer%d_sync_in_progress", i)) == 1;
            }
        }
        return false;
    }

    @VisibleForTesting
    protected long getDelaySeconds(long rdbSize) {
        long base = 3 * 60; // 3 minutes / per GB size
        long gb = 1024 * 1024 * 1024;
        return base * (rdbSize/gb + 1);
    }
}
