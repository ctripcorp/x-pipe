package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/**
 * @author yu
 * <p>
 * 2023/10/31
 */
@Component
public class DefaultKeeperSessionManager extends AbstractInstanceSessionManager implements KeeperSessionManager {

    @Override
    protected Set<HostPort> getInUseInstances() {
        DcMeta currentDcMeta = metaCache.getXpipeMeta().getDcs().get(currentDcId);
        if (currentDcMeta != null)
            return getSessionsForKeeper(currentDcMeta, getCurrentDcAllMeta(currentDcId));

        return null;
    }

    @Override
    protected HostPort getMonitorInstance(List<RedisMeta> redises, KeeperMeta keeper) {
        return new HostPort(keeper.getIp(), keeper.getPort());
    }
}
