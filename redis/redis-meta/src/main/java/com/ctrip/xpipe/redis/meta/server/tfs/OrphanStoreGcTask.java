package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerServiceFactory;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.MetaServerLeaderAware;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Periodic fallback GC for TFS keeper stores a {@code release()} (not {@code destroy()}) left behind
 * on disk with no live keeper anywhere pointing at them any more — migration/dead-keeper cleanup that
 * never got cleaned up by its own follow-up flow.
 * <p>
 * Runs leader-only (see {@link #isleader()}/{@link #notLeader()}) and defense-in-depth at every layer:
 * meta-server decides <b>candidates</b> from cluster/shard/keeper meta (never touches a file itself,
 * see §1-§2), the keeper container that actually executes a delete re-derives "is this really an
 * orphan" from the real file system before touching anything (see {@code KeeperContainerService}
 * {@code .gcOrphanStore} on the keeper side) — this class's candidate list is advisory, not authoritative.
 * <p>
 * Grouping: a {@code tfsFsId} (1:1 with {@code logicalBuId}) is one shared mount. Every container on it
 * sees the same {@code repl_<shardDbId>} directories, so one reachable representative per group is
 * enough to list them (§2 of the design) — no need to ask every container, and no need to match a
 * keeper's current container exactly (the occupied set is the union across the whole group).
 */
@Component
public class OrphanStoreGcTask implements MetaServerLeaderAware, TopElement {

    private static final Logger logger = LoggerFactory.getLogger(OrphanStoreGcTask.class);

    private static final String REPL_DIR_PREFIX = "repl_";

    @Autowired
    private MetaServerConfig config;

    @Autowired
    private DcMetaCache metaCache;

    @Autowired
    private CurrentClusterServer currentClusterServer;

    @Autowired
    private KeeperContainerServiceFactory keeperContainerServiceFactory;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private volatile ScheduledFuture<?> future;

    /** Runs {@code tfsFsId} groups in parallel. */
    private ExecutorService groupExecutor;

    @PostConstruct
    public void init() {
        groupExecutor = Executors.newFixedThreadPool(Math.max(1, config.getOrphanGcGroupParallelism()),
                XpipeThreadFactory.create("orphan-gc-group"));
    }

    @PreDestroy
    public void dispose() {
        groupExecutor.shutdownNow();
    }

    @Override
    public void isleader() {
        long intervalMilli = config.getOrphanGcRoundIntervalMilli();
        logger.info("[isleader][schedule orphan store gc]intervalMilli={}", intervalMilli);
        future = scheduled.scheduleWithFixedDelay(this::safeDoGcRound, intervalMilli, intervalMilli, TimeUnit.MILLISECONDS);
    }

    @Override
    public void notLeader() {
        logger.info("[notLeader][stop orphan store gc]");
        ScheduledFuture<?> current = future;
        if (current != null) {
            current.cancel(true);
            future = null;
        }
    }

    private void safeDoGcRound() {
        try {
            doGcRound();
        } catch (Throwable th) {
            logger.error("[doGcRound]", th);
        }
    }

    /**
     * One GC round. Every short-circuit below is "skip this round/group", never "treat as empty and
     * proceed" — an uncertain answer must never read as "nothing occupied, safe to delete".
     */
    @VisibleForTesting
    protected void doGcRound() {
        String requestId = newRequestId();

        if (!config.isOrphanGcEnabled()) {
            logger.debug("[doGcRound][{}] disabled, skip", requestId);
            return;
        }
        if (!isLeader()) {
            logger.info("[doGcRound][{}] not leader, skip", requestId);
            return;
        }
        if (!isMetaFreshEnough()) {
            logger.warn("[doGcRound][{}] meta not refreshed recently enough, abort this round", requestId);
            return;
        }

        Set<ClusterMeta> clusters = metaCache.getClusters();
        if (clusters == null || clusters.isEmpty()) {
            logger.warn("[doGcRound][{}] empty meta, abort this round", requestId);
            return;
        }

        List<KeeperContainerMeta> allContainers = metaCache.getKeeperContainers();
        Map<String, List<KeeperContainerMeta>> containersByFsId = groupTfsContainersByFsId(allContainers);
        if (containersByFsId.isEmpty()) {
            logger.info("[doGcRound][{}] no tfs containers known, skip", requestId);
            return;
        }

        Map<Long, KeeperContainerMeta> containersById = indexContainersById(allContainers);
        Map<String, Set<Long>> occupiedReplIdsByFsId = occupiedReplIdsByFsId(clusters, containersById);

        List<CompletableFuture<Void>> groupFutures = new ArrayList<>(containersByFsId.size());
        for (Map.Entry<String, List<KeeperContainerMeta>> entry : containersByFsId.entrySet()) {
            String fsId = entry.getKey();
            List<KeeperContainerMeta> containers = entry.getValue();
            Set<Long> occupied = occupiedReplIdsByFsId.getOrDefault(fsId, Collections.emptySet());
            groupFutures.add(CompletableFuture.runAsync(() -> {
                if (!isLeader()) {
                    logger.info("[doGcRound][{}][fsId={}] lost leadership, abort this group", requestId, fsId);
                    return;
                }
                doGcGroup(requestId, fsId, containers, occupied);
            }, groupExecutor).exceptionally(th -> {
                // One group's bug must never take down the rest of the round.
                logger.error("[doGcRound][{}][fsId={}] group task failed", requestId, fsId, th);
                return null;
            }));
        }
        CompletableFuture.allOf(groupFutures.toArray(new CompletableFuture[0])).join();
    }

    /**
     * One {@code tfsFsId} group: list the shared mount from one reachable representative, subtract
     * what current meta still occupies, then either log (dry-run) or ask the same representative to
     * gc each remaining candidate.
     */
    private void doGcGroup(String requestId, String fsId, List<KeeperContainerMeta> containers, Set<Long> occupied) {
        Pair<KeeperContainerMeta, List<String>> probed = findReachableRepresentative(requestId, fsId, containers);
        if (probed == null) {
            logger.warn("[doGcGroup][{}][fsId={}] no reachable container in this group, skip this round", requestId, fsId);
            return;
        }
        KeeperContainerMeta representative = probed.getKey();
        List<String> dirNames = probed.getValue();
        KeeperContainerService representativeService = keeperContainerServiceFactory.getOrCreateKeeperContainerService(representative);

        if (dirNames.isEmpty()) {
            logger.debug("[doGcGroup][{}][fsId={}] nothing on {}", requestId, fsId, representative);
            return;
        }

        List<Long> candidates = new ArrayList<>();
        for (String dirName : dirNames) {
            Long replId = parseReplId(dirName);
            if (replId != null && !occupied.contains(replId)) {
                candidates.add(replId);
            }
        }
        if (candidates.isEmpty()) {
            logger.debug("[doGcGroup][{}][fsId={}] {} dirs, none orphaned", requestId, fsId, dirNames.size());
            return;
        }
        logger.info("[doGcGroup][{}][fsId={}] {} dirs on {}, {} candidate(s) after meta filter: {}",
                requestId, fsId, dirNames.size(), representative, candidates.size(), candidates);

        if (config.isOrphanGcDryRun()) {
            logger.info("[doGcGroup][{}][fsId={}][dry-run] would gc {} candidate(s) via {}: {}",
                    requestId, fsId, candidates.size(), representative, candidates);
            return;
        }
        if (!isLeader()) {
            logger.info("[doGcGroup][{}][fsId={}] lost leadership before batch gc, abort", requestId, fsId);
            return;
        }
        gcBatch(requestId, fsId, representativeService, representative, candidates);
    }

    /** Tries containers in order; the first one that lists the shared mount is the representative. */
    private Pair<KeeperContainerMeta, List<String>> findReachableRepresentative(
            String requestId, String fsId, List<KeeperContainerMeta> containers) {
        for (KeeperContainerMeta candidate : containers) {
            try {
                List<String> dirNames = keeperContainerServiceFactory.getOrCreateKeeperContainerService(candidate).listStoreDirNames();
                return Pair.from(candidate, dirNames == null ? Collections.<String>emptyList() : dirNames);
            } catch (Throwable th) {
                logger.warn("[findReachableRepresentative][{}][fsId={}] {} unreachable, try next", requestId, fsId, candidate, th);
            }
        }
        return null;
    }

    /**
     * One request for the whole group's candidate list instead of one per replId — a group can have
     * dozens to low hundreds of candidates after a long-idle shared mount, and each replId already pays
     * for its own independent safety checks server-side (see {@code KeeperContainerService
     * .gcOrphanStore}); batching only removes the redundant network round trips, not any of those
     * per-replId checks.
     */
    private void gcBatch(String requestId, String fsId, KeeperContainerService service,
                          KeeperContainerMeta representative, List<Long> replIds) {
        try {
            List<GcOrphanStoreResult> results = service.gcOrphanStores(replIds, requestId);
            logger.info("[doGcGroup][{}][fsId={}] batch gc {} candidate(s) via {} -> {}",
                    requestId, fsId, replIds.size(), representative, results);
        } catch (Throwable th) {
            logger.warn("[doGcGroup][{}][fsId={}] batch gc call failed via {}, replIds={}",
                    requestId, fsId, representative, replIds, th);
        }
    }

    /**
     * TFS containers in the current dc's meta, grouped by {@code tfsFsId} — a 1:1 proxy for
     * {@code logicalBuId} (one shared TFS mount). BM containers, and any container missing a
     * {@code tfsFsId}, never enter a group: {@link KeeperMediumResolver#isBm} fails closed toward
     * "treat as TFS" on ambiguity, which is exactly backwards for deciding who to list a destructive
     * candidate set from, so this filters the other way — only a container clearly identified as TFS
     * (not BM) with a concrete fsId joins a group.
     */
    @VisibleForTesting
    protected Map<String, List<KeeperContainerMeta>> groupTfsContainersByFsId(List<KeeperContainerMeta> allContainers) {
        Map<String, List<KeeperContainerMeta>> result = new HashMap<>();
        for (KeeperContainerMeta container : allContainers) {
            if (KeeperMediumResolver.isBm(container) || StringUtil.isEmpty(container.getTfsFsId())) {
                continue;
            }
            result.computeIfAbsent(container.getTfsFsId(), k -> new ArrayList<>()).add(container);
        }
        return result;
    }

    /**
     * {@code container.getId() -> container}, built once per round from the same list
     * {@link #groupTfsContainersByFsId} already walked, so {@link #occupiedReplIdsByFsId} can resolve
     * each keeper's container by {@code keeperContainerId} with a map lookup instead of
     * {@code DcMetaCache.getKeeperContainer(KeeperMeta)} — a locked linear scan plus a defensive clone
     * per call, which turns a cluster with many shards/keepers into O(keepers × containers) of lock
     * contention and allocation for no reason: the id is already right there on the KeeperMeta.
     */
    @VisibleForTesting
    protected Map<Long, KeeperContainerMeta> indexContainersById(List<KeeperContainerMeta> allContainers) {
        Map<Long, KeeperContainerMeta> result = new HashMap<>(allContainers.size());
        for (KeeperContainerMeta container : allContainers) {
            result.put(container.getId(), container);
        }
        return result;
    }

    /**
     * For every keeper in current (non-purged) meta whose container is TFS, its shard's replId
     * (replId == shardDbId) is occupied in that container's {@code tfsFsId} group. Walking meta this
     * way, rather than trusting any one container's live-keeper map, is what makes "ask one
     * representative" safe — the occupied set does not depend on which container within the group
     * each keeper instance currently happens to be on.
     */
    @VisibleForTesting
    protected Map<String, Set<Long>> occupiedReplIdsByFsId(Set<ClusterMeta> clusters, Map<Long, KeeperContainerMeta> containersById) {
        Map<String, Set<Long>> result = new HashMap<>();
        for (ClusterMeta cluster : clusters) {
            if (cluster.getAllShards() == null) {
                continue;
            }
            for (ShardMeta shard : cluster.getAllShards().values()) {
                if (shard.getKeepers() == null) {
                    continue;
                }
                for (KeeperMeta keeper : shard.getKeepers()) {
                    KeeperContainerMeta container = containersById.get(keeper.getKeeperContainerId());
                    if (container == null || KeeperMediumResolver.isBm(container) || StringUtil.isEmpty(container.getTfsFsId())) {
                        continue;
                    }
                    result.computeIfAbsent(container.getTfsFsId(), k -> new HashSet<>()).add(shard.getDbId());
                }
            }
        }
        return result;
    }

    @VisibleForTesting
    protected Long parseReplId(String dirName) {
        if (dirName == null || !dirName.startsWith(REPL_DIR_PREFIX)) {
            return null;
        }
        try {
            return Long.parseLong(dirName.substring(REPL_DIR_PREFIX.length()));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private boolean isLeader() {
        return currentClusterServer.isLeader();
    }

    /** See {@link DcMetaCache#getLastSuccessfulLoadMilli()} — distinct from "meta actually changed". */
    private boolean isMetaFreshEnough() {
        long age = System.currentTimeMillis() - metaCache.getLastSuccessfulLoadMilli();
        return age <= config.getOrphanGcMetaFreshMilli();
    }

    private String newRequestId() {
        return "orphan-gc-" + UUID.randomUUID();
    }

    @VisibleForTesting
    protected void setConfig(MetaServerConfig config) {
        this.config = config;
    }

    @VisibleForTesting
    protected void setMetaCache(DcMetaCache metaCache) {
        this.metaCache = metaCache;
    }

    @VisibleForTesting
    protected void setCurrentClusterServer(CurrentClusterServer currentClusterServer) {
        this.currentClusterServer = currentClusterServer;
    }

    @VisibleForTesting
    protected void setKeeperContainerServiceFactory(KeeperContainerServiceFactory keeperContainerServiceFactory) {
        this.keeperContainerServiceFactory = keeperContainerServiceFactory;
    }

    @VisibleForTesting
    protected void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @VisibleForTesting
    protected void setGroupExecutor(ExecutorService groupExecutor) {
        this.groupExecutor = groupExecutor;
    }
}
