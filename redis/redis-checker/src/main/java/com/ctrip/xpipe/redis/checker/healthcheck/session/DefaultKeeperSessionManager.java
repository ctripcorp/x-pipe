package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.LinkedList;
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
    public Set<HostPort> getInUseInstances() {
        return getInUseKeepers();
    }

    Set<HostPort> getInUseKeepers() {
        Set<HostPort> keeperInUse = new HashSet<>();
        List<DcMeta> dcMetas = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
        if(dcMetas.isEmpty())	return null;

        for (DcMeta dcMeta : dcMetas) {
            if(dcMeta == null || !dcMeta.getId().equalsIgnoreCase(currentDcId))	continue;

            DcMeta currentDcAllMeta = getCurrentDcAllMeta(currentDcId);
            getSessionsForKeeper(dcMeta, currentDcAllMeta, keeperInUse, false);
        }

        return keeperInUse;
    }



}
