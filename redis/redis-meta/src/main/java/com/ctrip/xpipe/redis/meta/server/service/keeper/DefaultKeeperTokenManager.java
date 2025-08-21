package com.ctrip.xpipe.redis.meta.server.service.keeper;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.MetaServerLeaderAware;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * Feb 26, 2020
 */
//@Component
public class DefaultKeeperTokenManager implements KeeperTokenManager, MetaServerLeaderAware {

    private AtomicBoolean isKeeperRateLimitOpen = new AtomicBoolean(true);

    private MetaServerConfig metaServerConfig;

    @Override
    public MetaServerKeeperService.KeeperContainerTokenStatusResponse refreshKeeperTokenStatus(MetaServerKeeperService.KeeperContainerTokenStatusRequest request) {
        return new MetaServerKeeperService.KeeperContainerTokenStatusResponse(3);
    }

    @Override
    public boolean closeKeeperRateLimit() {
        return isKeeperRateLimitOpen.compareAndSet(true, false);
    }

    @Override
    public void isleader() {

    }

    @Override
    public void notLeader() {

    }
}
