package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Owns Checker sessions for Keepers independently from Redis sessions.
 *
 * The D43 selector and explicit release semantics are introduced in Phase KS.
 */
@Component
public class DefaultKeeperSessionManager extends AbstractInstanceSessionManager implements KeeperSessionManager {

    @Override
    public RedisSession findOrCreateSession(HostPort hostPort) {
        return findOrCreateSession(new DefaultEndPoint(hostPort.getHost(), hostPort.getPort()));
    }

    @Override
    protected Set<HostPort> getInUseInstances() {
        Set<HostPort> keepersInUse = new HashSet<>();
        List<DcMeta> dcMetas = new LinkedList<>(metaCache.getXpipeMeta().getDcs().values());
        if (dcMetas.isEmpty()) return null;

        for (DcMeta dcMeta : dcMetas) {
            if (dcMeta == null) break;
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                        keepersInUse.add(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()));
                    }
                }
            }
        }
        return keepersInUse;
    }
}
