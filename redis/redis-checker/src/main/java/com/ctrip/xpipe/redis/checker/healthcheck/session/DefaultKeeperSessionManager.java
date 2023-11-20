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
        DcMeta currentDcAllMeta = metaCache.getXpipeMeta().getDcs().get(currentDcId);
        if (currentDcAllMeta != null)
            return getSessionsForKeeper(currentDcAllMeta, this.currentDcAllMeta.getCurrentDcAllMeta());

        return null;
    }

    @Override
    protected HostPort getMonitorInstance(List<RedisMeta> redises, KeeperMeta keeper) {
        return new HostPort(keeper.getIp(), keeper.getPort());
    }
}
