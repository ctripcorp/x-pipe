package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.HealthState;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.tfs.TfsKeeperUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Strategy-aware keeper active election: priority desc with ZK order tie-break;
 * {@link KeeperElectStrategy#BM_PREFER} prefers non-TFS keepers.
 *
 * <p>{@link #filterHealthy} first narrows candidates by medium and health state.
 * A non-empty input always elects a keeper.
 */
public class StrategyAwareKeeperActiveElectAlgorithm extends AbstractActiveElectAlgorithm {

    private final KeeperElectStrategy strategy;
    private final DcMetaCache dcMetaCache;

    public StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy strategy, DcMetaCache dcMetaCache) {
        this.strategy = strategy;
        this.dcMetaCache = dcMetaCache;
    }

    @Override
    public KeeperMeta select(Long clusterDbId, Long shardDbId, List<KeeperMeta> toBeSelected) {
        if (toBeSelected.isEmpty()) {
            return null;
        }

        List<KeeperMeta> filtered = filterHealthy(clusterDbId, shardDbId, toBeSelected);
        List<KeeperMeta> candidates = filterElectable(filtered);
        if (candidates.isEmpty()) {
            KeeperMeta fallback = filtered.get(0);
            logger.warn("[select][no keeper with priority>0, fallback first by zk order]cluster_{},shard_{},fallback={},keepers={}",
                    clusterDbId, shardDbId, fallback, filtered);
            return fallback;
        }

        if (strategy == KeeperElectStrategy.BM_PREFER) {
            List<KeeperMeta> bmCandidates = filterBm(candidates);
            if (!bmCandidates.isEmpty()) {
                candidates = bmCandidates;
            }
        }

        return selectByPriorityAndZkOrder(candidates);
    }

    /**
     * Narrows candidates by medium and TFS health; never returns empty for non-empty input.
     * <ul>
     *     <li>All BM: return the original list.</li>
     *     <li>All TFS: return healthy TFS keepers; if all are unhealthy, return the original list.</li>
     *     <li>Mixed: return BM keepers when fewer than two healthy TFS keepers survive, or when
     *     any surviving TFS keeper is unhealthy; otherwise return the original list.</li>
     * </ul>
     * A mixed shard therefore requires at least two healthy surviving TFS keepers before TFS can be active.
     */
    private List<KeeperMeta> filterHealthy(Long clusterDbId, Long shardDbId, List<KeeperMeta> keepers) {
        List<KeeperMeta> bmKeepers = new ArrayList<>();
        List<KeeperMeta> healthyTfsKeepers = new ArrayList<>();
        int tfsCount = 0;

        for (KeeperMeta keeperMeta : keepers) {
            if (!isKeeperTfs(keeperMeta)) {
                bmKeepers.add(keeperMeta);
                continue;
            }

            tfsCount++;
            if (HealthState.isElectable(keeperMeta.getHealthState())) {
                healthyTfsKeepers.add(keeperMeta);
            }
        }

        if (tfsCount == 0) {
            return keepers;
        }

        if (bmKeepers.isEmpty()) {
            if (healthyTfsKeepers.isEmpty()) {
                logger.warn("[filterHealthy][all TFS unhealthy, elect among all]cluster_{},shard_{},keepers={}",
                        clusterDbId, shardDbId, keepers);
                return keepers;
            }
            return healthyTfsKeepers;
        }

        if (healthyTfsKeepers.size() <= 1 || healthyTfsKeepers.size() < tfsCount) {
            logger.info("[filterHealthy][insufficient healthy TFS in mixed shard, BM only]cluster_{},shard_{},keepers={}",
                    clusterDbId, shardDbId, keepers);
            return bmKeepers;
        }

        return keepers;
    }

    private List<KeeperMeta> filterBm(List<KeeperMeta> keepers) {
        List<KeeperMeta> bmKeepers = new ArrayList<>();
        for (KeeperMeta keeperMeta : keepers) {
            if (!isKeeperTfs(keeperMeta)) {
                bmKeepers.add(keeperMeta);
            }
        }
        return bmKeepers;
    }

    private List<KeeperMeta> filterElectable(List<KeeperMeta> keepers) {
        List<KeeperMeta> electable = new ArrayList<>();
        for (KeeperMeta keeperMeta : keepers) {
            if (keeperPriority(keeperMeta) > 0) {
                electable.add(keeperMeta);
            }
        }
        return electable;
    }

    private KeeperMeta selectByPriorityAndZkOrder(List<KeeperMeta> candidates) {
        KeeperMeta best = null;
        int bestPriority = -1;
        for (KeeperMeta keeperMeta : candidates) {
            int priority = keeperPriority(keeperMeta);
            if (priority > bestPriority) {
                bestPriority = priority;
                best = keeperMeta;
            }
        }
        return best;
    }

    /** An unknown container (stale keeper whose container left dcMeta) counts as non-TFS instead of throwing. */
    private boolean isKeeperTfs(KeeperMeta keeperMeta) {
        return TfsKeeperUtils.isTfsKeeper(keeperMeta, dcMetaCache);
    }

    private int keeperPriority(KeeperMeta keeperMeta) {
        Integer priority = keeperMeta.getPriority();
        // Meta load (D28) already normalizes; null should not appear on elect path.
        return priority != null ? priority : 0;
    }
}
