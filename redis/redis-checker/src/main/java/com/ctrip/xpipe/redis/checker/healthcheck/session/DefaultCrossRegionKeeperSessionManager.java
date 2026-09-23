package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

/**
 * Owns Checker sessions for the Keepers an {@code InfoReplIdAction} reads, in a backup dc whose
 * cluster is cross-region.
 *
 * The Keeper is local to the checker -- it sits in this dc's cluster entry, which is why the action
 * finds it with {@code getKeeperOfDcClusterShard(redisDc, ...)}. What excludes it from
 * {@link DefaultKeeperSessionManager} is the <em>cluster's</em> {@code activeDc}: the
 * {@link KeeperCheckSelector} it is driven by requires {@code activeDc == currentDc}, which fails
 * for every cluster this dc only backs up. Nor does {@link DefaultRedisSessionManager} claim it,
 * since its in-use set holds only redis. Without this manager those sessions were recycled every
 * cleanup cycle and rebuilt on the next check.
 *
 * {@code findOrCreateSession(HostPort)} is left to the parent, so endpoints resolve through
 * {@code HealthCheckEndpointFactory} like the paired redis instance's does. That is a consistency
 * choice, not a routing requirement: this Keeper is local, so no proxy route is expected.
 */
@Component
public class DefaultCrossRegionKeeperSessionManager extends AbstractInstanceSessionManager
        implements CrossRegionKeeperSessionManager {

    /**
     * The keepers of every one-way cluster whose active dc is cross-region, as seen from this dc.
     * Mirrors the loading rule of {@code InfoReplIdActionFactory.supportInstnace}, so a session is
     * held for exactly the keepers an action may query.
     */
    @Override
    protected Set<HostPort> getInUseInstances() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        // null / empty meta means the expectation is unknown, not "nothing is in use"
        if (xpipeMeta == null || xpipeMeta.getDcs() == null || xpipeMeta.getDcs().isEmpty()) {
            return null;
        }

        DcMeta currentDcMeta = xpipeMeta.getDcs().get(currentDcId);
        if (currentDcMeta == null) {
            return null;
        }

        Set<HostPort> keepersInUse = new HashSet<>();
        for (ClusterMeta clusterMeta : currentDcMeta.getClusters().values()) {
            if (!isCrossRegionCluster(clusterMeta)) {
                continue;
            }
            for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                    keepersInUse.add(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()));
                }
            }
        }
        return keepersInUse;
    }

    private boolean isCrossRegionCluster(ClusterMeta clusterMeta) {
        if (ClusterType.lookup(clusterMeta.getType()) != ClusterType.ONE_WAY) {
            return false;
        }
        String activeDc = clusterMeta.getActiveDc();
        return !StringUtil.isEmpty(activeDc) && metaCache.isCrossRegion(currentDcId, activeDc);
    }

    /**
     * The in-use set is a precise expectation here: an empty one means no cross-region cluster
     * matched, so every session must be recycled rather than kept alive until the meta grows again.
     */
    @Override
    protected boolean cleanUpOnEmptyInUseInstances() {
        return true;
    }
}
