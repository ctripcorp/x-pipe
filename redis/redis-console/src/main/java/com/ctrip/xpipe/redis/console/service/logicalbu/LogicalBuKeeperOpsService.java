package com.ctrip.xpipe.redis.console.service.logicalbu;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.core.meta.HealthState;
import com.ctrip.xpipe.redis.core.metaserver.DcMetaReloadResult;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpStatusCodeException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Logical-BU level keeper medium switching, executed synchronously. No precheck: the election algorithm on
 * MetaServer is the safety net (priority 0 keepers are not electable, unhealthy TFS keepers are filtered out).
 * <ul>
 *     <li>{@link #switchBM}: LOGICAL_BU_TBL.priority → 0, TFS keepers of the BU stop being electable.</li>
 *     <li>{@link #switchRestore}: LOGICAL_BU_TBL.priority → NULL, TFS keepers go back to their own
 *     REDIS_TBL.keeper_priority, i.e. the state before switchBM.</li>
 *     <li>{@link #resetHealth}: every keeper on the BU's TFS keepercontainers: RECOVERED_PENDING → HEALTHY through the
 *     keepercontainer; priority untouched. Keepers publish the new state to ZK themselves.</li>
 * </ul>
 *
 * <ol>
 *     <li>LOAD (once per request): keepercontainers and dc names. The IDCs of a BU are the IDCs of its TFS
 *     keepercontainers.</li>
 *     <li>UPDATE_DB (per BU, BUs in parallel): CAS LOGICAL_BU_TBL.priority to the target (skipped when already there).</li>
 *     <li>NOTIFY_META (once per IDC, after all BUs updated): {@code POST /api/dcmeta/reload} so every MetaServer of
 *     the IDC pulls DcMeta now through its periodic-refresh path. MetaServer re-elects priority-changed shards through
 *     its rate limiter ({@code keeper.priority.reelect.rate.per.second}).</li>
 * </ol>
 * Every external call goes through {@link LayeredCaller}, so failures carry layer / stage / target.
 */
@Service
public class LogicalBuKeeperOpsService {

    private static final Logger logger = LoggerFactory.getLogger(LogicalBuKeeperOpsService.class);

    static final Integer TFS_PRIORITY_MIGRATED_TO_BM = 0;
    /** NULL: no override, TFS keepers use REDIS_TBL.keeper_priority again */
    static final Integer TFS_PRIORITY_RESTORED = null;
    static final int BU_CONCURRENCY = 4;
    /** MetaServer side: jitter + getDcMeta + retry + peer fan-out; client read timeout is 60s. */
    static final long RELOAD_WAIT_MILLI = 90_000;
    static final int KEEPER_CONCURRENCY = 8;
    static final long KEEPER_WAIT_BASE_MILLI = 30_000;
    static final long KEEPER_WAIT_PER_CALL_MILLI = 100;
    static final String RECOVER_OK = "OK";

    @Autowired
    private LogicalBuService logicalBuService;

    @Autowired
    private KeeperContainerService keeperContainerService;

    @Autowired
    private DcService dcService;

    @Autowired
    private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;

    @Autowired
    private KeeperContainerCheckerService keeperContainerCheckerService;

    @Autowired
    private ConsoleConfig consoleConfig;

    private final Set<Long> runningLogicalBus = ConcurrentHashMap.newKeySet();

    /** Per-BU DB updates and per-IDC reloads; the two phases never wait on tasks of their own phase. */
    private ExecutorService executor;

    /** keepercontainer HTTP calls (list + recover), shared by all BUs of all requests */
    private ExecutorService keeperExecutor;

    @PostConstruct
    public void init() {
        executor = Executors.newFixedThreadPool(BU_CONCURRENCY, XpipeThreadFactory.create("LogicalBuKeeperOps"));
        keeperExecutor = Executors.newFixedThreadPool(KEEPER_CONCURRENCY, XpipeThreadFactory.create("LogicalBuKeeperOps-keeper"));
    }

    @PreDestroy
    public void destroy() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (keeperExecutor != null) {
            keeperExecutor.shutdownNow();
        }
    }

    /** Synchronous switch to BM. Each logical BU succeeds or fails on its own. */
    public KeeperOpResult switchBM(KeeperOpRequest request) {
        return switchPriority(request, TFS_PRIORITY_MIGRATED_TO_BM, "switchBM");
    }

    /** Synchronous restore: undo switchBM, TFS keepers use their own keeper_priority again. */
    public KeeperOpResult switchRestore(KeeperOpRequest request) {
        return switchPriority(request, TFS_PRIORITY_RESTORED, "switchRestore");
    }

    /**
     * Reset TFS keeper health of the BUs: list keepers of every TFS keepercontainer of the BU, then ask each
     * keepercontainer to recover them (RECOVERED_PENDING → HEALTHY; HEALTHY is a no-op; DOWN / SICK are rejected).
     * LOGICAL_BU_TBL.priority is not touched, so with priority 0 the recovered TFS keepers stay backup.
     */
    public KeeperOpResult resetHealth(KeeperOpRequest request) {
        validate(request);
        Set<Long> ids = new LinkedHashSet<>(request.getLogicalBuIds());
        SharedContext ctx = loadShared();

        Map<Long, Future<LogicalBuOpResult>> futures = new LinkedHashMap<>();
        for (Long id : ids) {
            futures.put(id, executor.submit(() -> resetHealthOne(id, ctx)));
        }
        List<LogicalBuOpResult> results = new ArrayList<>();
        for (Map.Entry<Long, Future<LogicalBuOpResult>> entry : futures.entrySet()) {
            try {
                results.add(entry.getValue().get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                results.add(internalError(entry.getKey(), e));
            } catch (ExecutionException e) {
                results.add(internalError(entry.getKey(), e.getCause()));
            }
        }
        return KeeperOpResult.of(results);
    }

    private LogicalBuOpResult resetHealthOne(long logicalBuId, SharedContext ctx) {
        LogicalBuOpResult result = new LogicalBuOpResult(logicalBuId);
        if (!runningLogicalBus.add(logicalBuId)) {
            result.addFailure(OpFailure.console(OpStage.LOAD, OpErrorCode.OPERATION_CONFLICT, null, null));
            result.finish(LogicalBuOpResult.State.BLOCKED);
            return result;
        }
        try {
            if (ctx.loadFailure != null) {
                result.addFailure(ctx.loadFailure);
                result.finish(LogicalBuOpResult.State.BLOCKED);
                return result;
            }
            LogicalBuModel logicalBu = loadLogicalBu(logicalBuId, result);
            if (logicalBu == null) {
                result.finish(LogicalBuOpResult.State.BLOCKED);
                return result;
            }
            result.setLogicalBuName(logicalBu.getName());
            result.setPriorityBefore(logicalBu.getPriority());
            result.setPriorityAfter(logicalBu.getPriority());

            List<KeepercontainerTbl> containers = tfsContainersOf(logicalBuId, ctx);
            for (KeepercontainerTbl container : containers) {
                String dc = ctx.dcNames.get(container.getKeepercontainerDc());
                if (dc != null) {
                    result.getIdcs().add(dc);
                }
            }
            recoverContainers(containers, ctx, result);
            result.finish(result.hasFailures() ? LogicalBuOpResult.State.FAILED : LogicalBuOpResult.State.SUCCESS);
            return result;
        } catch (Throwable th) {
            logger.error("[resetHealth][{}] unexpected failure", logicalBuId, th);
            result.addFailure(OpFailure.console(OpStage.RECOVER_HEALTH, OpErrorCode.INTERNAL_ERROR, null, LayeredCaller.rootMessage(th)));
            result.finish(LogicalBuOpResult.State.FAILED);
            return result;
        } finally {
            runningLogicalBus.remove(logicalBuId);
            logger.info("[resetHealth][{}] state={}, keepers={}/{}, failures={}", logicalBuId, result.getState(),
                    result.getSuccessKeeperCount(), result.getKeeperCount(), result.getFailures());
        }
    }

    /** step 1: list keepers per container (parallel); step 2: recover each keeper (parallel, bounded) */
    private void recoverContainers(List<KeepercontainerTbl> containers, SharedContext ctx, LogicalBuOpResult result)
            throws InterruptedException {
        Map<KeepercontainerTbl, Future<List<KeeperInstanceMeta>>> lists = new LinkedHashMap<>();
        for (KeepercontainerTbl container : containers) {
            String dc = ctx.dcNames.get(container.getKeepercontainerDc());
            lists.put(container, keeperExecutor.submit(() -> LayeredCaller.keeper(OpStage.RECOVER_HEALTH,
                    containerTarget(container), dc, () -> keeperContainerCheckerService.getAllKeepers(dc,
                            container.getKeepercontainerIp(), container.getKeepercontainerHostName(),
                            container.getKeepercontainerPort()))));
        }

        long listDeadline = System.currentTimeMillis() + KEEPER_WAIT_BASE_MILLI
                + containers.size() * KEEPER_WAIT_PER_CALL_MILLI;
        result.setContainerCount(containers.size());
        // containers whose keeper list could not be loaded never count as successful
        Set<String> failedContainers = new HashSet<>();
        Map<String, Future<OpFailure>> recovers = new LinkedHashMap<>();
        for (Map.Entry<KeepercontainerTbl, Future<List<KeeperInstanceMeta>>> entry : lists.entrySet()) {
            KeepercontainerTbl container = entry.getKey();
            String dc = ctx.dcNames.get(container.getKeepercontainerDc());
            String target = containerTarget(container);
            List<KeeperInstanceMeta> keepers;
            try {
                keepers = entry.getValue().get(Math.max(1, listDeadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                entry.getValue().cancel(true);
                result.addFailure(new OpFailure(OpLayer.KEEPER, OpStage.RECOVER_HEALTH, OpErrorCode.KEEPER_TIMEOUT,
                        target, dc, "list keepers timeout"));
                failedContainers.add(target);
                continue;
            } catch (ExecutionException e) {
                result.addFailure(toKeeperFailure(e.getCause(), target, dc));
                failedContainers.add(target);
                continue;
            }
            if (keepers == null) {
                continue;
            }
            for (KeeperInstanceMeta keeper : keepers) {
                Long replId = keeper.getReplId();
                if (replId == null) {
                    continue;
                }
                String scope = dc + "/replId_" + replId;
                recovers.put(target + "#" + replId, keeperExecutor.submit(() -> recoverOne(container, dc, target, scope, replId)));
            }
        }

        result.setKeeperCount(recovers.size());
        long recoverDeadline = System.currentTimeMillis() + KEEPER_WAIT_BASE_MILLI
                + recovers.size() * KEEPER_WAIT_PER_CALL_MILLI;
        int success = 0;
        for (Map.Entry<String, Future<OpFailure>> entry : recovers.entrySet()) {
            String target = entry.getKey().substring(0, entry.getKey().lastIndexOf('#'));
            OpFailure failure;
            try {
                failure = entry.getValue().get(Math.max(1, recoverDeadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                entry.getValue().cancel(true);
                failure = new OpFailure(OpLayer.KEEPER, OpStage.RECOVER_HEALTH, OpErrorCode.KEEPER_TIMEOUT,
                        target, entry.getKey(), "recover timeout");
            } catch (ExecutionException e) {
                failure = toKeeperFailure(e.getCause(), target, entry.getKey());
            }
            if (failure == null) {
                success++;
            } else {
                result.addFailure(failure);
                failedContainers.add(target);
            }
        }
        result.setSuccessKeeperCount(success);
        int successContainers = 0;
        for (KeepercontainerTbl container : containers) {
            if (!failedContainers.contains(containerTarget(container))) {
                successContainers++;
            }
        }
        result.setSuccessContainerCount(successContainers);
    }

    /** @return null when the keepercontainer answered exactly "OK" */
    private OpFailure recoverOne(KeepercontainerTbl container, String dc, String target, String scope, Long replId) {
        String answer = LayeredCaller.keeper(OpStage.RECOVER_HEALTH, target, scope,
                () -> keeperContainerCheckerService.recoverHealthState(dc, container.getKeepercontainerIp(),
                        container.getKeepercontainerHostName(), container.getKeepercontainerPort(), replId));
        if (answer != null && RECOVER_OK.equals(answer.trim())) {
            return null;
        }
        // e.g. "Recover health for ... rejected in state SICK" / "keeper doesn't exist"
        return new OpFailure(OpLayer.KEEPER, OpStage.RECOVER_HEALTH, OpErrorCode.RECOVERY_REJECTED, target, scope, answer);
    }

    private OpFailure toKeeperFailure(Throwable cause, String target, String scope) {
        if (cause instanceof KeeperOpException) {
            return ((KeeperOpException) cause).getFailure();
        }
        return LayeredCaller.classifyKeeper(OpStage.RECOVER_HEALTH, target, scope, cause);
    }

    private static String containerTarget(KeepercontainerTbl container) {
        String host = container.getKeepercontainerHostName() == null || container.getKeepercontainerHostName().isEmpty()
                ? container.getKeepercontainerIp() : container.getKeepercontainerHostName();
        return "keepercontainer@" + host + ":" + container.getKeepercontainerPort();
    }

    private List<KeepercontainerTbl> tfsContainersOf(long logicalBuId, SharedContext ctx) {
        List<KeepercontainerTbl> result = new ArrayList<>();
        for (KeepercontainerTbl container : ctx.containers) {
            if (container.getLogicalBuId() == logicalBuId
                    && KeeperDiskTypeUtils.isTfs(container.getKeepercontainerDiskType())) {
                result.add(container);
            }
        }
        return result;
    }

    /**
     * Read-only: for each BU, ask every TFS keepercontainer for its keepers ({@code GET /keepers}) and classify the
     * container by the worst published keeper health: DOWN &gt; PENDING &gt; HEALTHY; UNKNOWN when it cannot be asked.
     * One HTTP call per container, no DB query per keeper. No BU lock: it can run next to a switch.
     */
    public KeeperStateResult keeperState(KeeperOpRequest request) {
        validate(request);
        SharedContext ctx = loadShared();
        KeeperStateResult response = new KeeperStateResult();

        Map<KeepercontainerTbl, Future<List<KeeperInstanceMeta>>> lists = new LinkedHashMap<>();
        Map<Long, KeeperStateResult.BuState> byBu = new LinkedHashMap<>();
        for (Long id : new LinkedHashSet<>(request.getLogicalBuIds())) {
            KeeperStateResult.BuState bu = new KeeperStateResult.BuState(id);
            byBu.put(id, bu);
            response.getResults().add(bu);
            if (ctx.loadFailure != null) {
                bu.setFailure(ctx.loadFailure);
                continue;
            }
            for (KeepercontainerTbl container : tfsContainersOf(id, ctx)) {
                String dc = ctx.dcNames.get(container.getKeepercontainerDc());
                lists.put(container, keeperExecutor.submit(() -> LayeredCaller.keeper(OpStage.LOAD,
                        containerTarget(container), dc, () -> keeperContainerCheckerService.getAllKeepers(dc,
                                container.getKeepercontainerIp(), container.getKeepercontainerHostName(),
                                container.getKeepercontainerPort()))));
            }
        }
        if (ctx.loadFailure == null) {
            Map<Long, String> names = loadBuNames(byBu.keySet());
            for (KeeperStateResult.BuState bu : byBu.values()) {
                bu.setLogicalBuName(names.get(bu.getLogicalBuId()));
            }
        }

        long deadline = System.currentTimeMillis() + KEEPER_WAIT_BASE_MILLI + lists.size() * KEEPER_WAIT_PER_CALL_MILLI;
        for (Map.Entry<KeepercontainerTbl, Future<List<KeeperInstanceMeta>>> entry : lists.entrySet()) {
            KeepercontainerTbl container = entry.getKey();
            String dc = ctx.dcNames.get(container.getKeepercontainerDc());
            KeeperStateResult.ContainerInfo info = containerInfo(container);
            try {
                classify(info, entry.getValue().get(Math.max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS));
            } catch (TimeoutException e) {
                entry.getValue().cancel(true);
                unknown(info, OpErrorCode.KEEPER_TIMEOUT.name() + " list keepers timeout");
            } catch (ExecutionException e) {
                OpFailure failure = toKeeperFailure(e.getCause(), containerTarget(container), dc);
                unknown(info, failure.getCode() + " " + (failure.getDetail() == null ? "" : failure.getDetail()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                unknown(info, "interrupted");
            }
            byBu.get(container.getLogicalBuId()).idc(dc == null ? "-" : dc).add(info);
        }
        return response;
    }

    private Map<Long, String> loadBuNames(Set<Long> ids) {
        Map<Long, String> names = new HashMap<>();
        try {
            for (LogicalBuModel bu : LayeredCaller.db(OpStage.LOAD, null, () -> logicalBuService.findAll())) {
                if (ids.contains(bu.getId())) {
                    names.put(bu.getId(), bu.getName());
                }
            }
        } catch (KeeperOpException e) {
            // names are cosmetic; states are still returned
            logger.warn("[keeperState] load names failed: {}", e.getFailure());
        }
        return names;
    }

    private static KeeperStateResult.ContainerInfo containerInfo(KeepercontainerTbl container) {
        String hostname = container.getKeepercontainerHostName();
        String host = hostname == null || hostname.isEmpty() ? container.getKeepercontainerIp() : hostname;
        return new KeeperStateResult.ContainerInfo()
                .setId(container.getKeepercontainerId())
                .setIp(container.getKeepercontainerIp())
                .setHostname(hostname)
                .setAddress(host + ":" + container.getKeepercontainerPort());
    }

    /** Published health is HEALTHY / DOWN / RECOVERED_PENDING; null or legacy values count as HEALTHY like election. */
    @VisibleForTesting
    static void classify(KeeperStateResult.ContainerInfo info, List<KeeperInstanceMeta> keepers) {
        int down = 0;
        int pending = 0;
        int total = 0;
        if (keepers != null) {
            for (KeeperInstanceMeta keeper : keepers) {
                total++;
                String health = keeper.getKeeperMeta() == null ? null : keeper.getKeeperMeta().getHealthState();
                switch (HealthState.parse(health)) {
                    case DOWN: down++; break;
                    case RECOVERED_PENDING: pending++; break;
                    // SICK counts as healthy: the keeper still serves, only RECOVERED_PENDING needs a reset
                    default:
                }
            }
        }
        info.setKeeperCount(total).setDownKeeperCount(down).setPendingKeeperCount(pending)
                .setState(down > 0 ? KeeperStateResult.ContainerState.DOWN
                        : pending > 0 ? KeeperStateResult.ContainerState.PENDING
                        : KeeperStateResult.ContainerState.HEALTHY);
    }

    private static void unknown(KeeperStateResult.ContainerInfo info, String reason) {
        info.setState(KeeperStateResult.ContainerState.UNKNOWN).setReason(reason);
    }

    private KeeperOpResult switchPriority(KeeperOpRequest request, Integer target, String op) {
        validate(request);
        Set<Long> ids = new LinkedHashSet<>(request.getLogicalBuIds());
        SharedContext ctx = loadShared();

        // phase 1, BUs in parallel: DB update
        Map<Long, Future<Prepared>> futures = new LinkedHashMap<>();
        for (Long id : ids) {
            futures.put(id, executor.submit(() -> prepare(id, target, ctx)));
        }
        List<Prepared> prepared = new ArrayList<>();
        for (Map.Entry<Long, Future<Prepared>> entry : futures.entrySet()) {
            try {
                prepared.add(entry.getValue().get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                prepared.add(new Prepared(internalError(entry.getKey(), e), false));
            } catch (ExecutionException e) {
                prepared.add(new Prepared(internalError(entry.getKey(), e.getCause()), false));
            }
        }

        try {
            // phase 2: one reload per IDC touched by any BU whose priority is now 0
            Set<String> dcs = new TreeSet<>();
            for (Prepared p : prepared) {
                if (p.updated) {
                    dcs.addAll(p.result.getIdcs());
                }
            }
            Map<String, OpFailure> reloadFailures = reloadDcs(dcs);

            for (Prepared p : prepared) {
                if (!p.updated) {
                    continue;
                }
                for (String dc : p.result.getIdcs()) {
                    OpFailure failure = reloadFailures.get(dc);
                    if (failure != null) {
                        p.result.addFailure(failure);
                    }
                }
                p.result.finish(p.result.hasFailures() ? LogicalBuOpResult.State.FAILED : LogicalBuOpResult.State.SUCCESS);
            }
        } finally {
            for (Prepared p : prepared) {
                if (p.locked) {
                    runningLogicalBus.remove(p.result.getLogicalBuId());
                }
                logger.info("[{}][{}] target={}, state={}, idcs={}, failures={}", op, p.result.getLogicalBuId(), target, p.result.getState(),
                        p.result.getIdcs(), p.result.getFailures());
            }
        }

        List<LogicalBuOpResult> results = new ArrayList<>();
        for (Prepared p : prepared) {
            results.add(p.result);
        }
        return KeeperOpResult.of(results);
    }

    private void validate(KeeperOpRequest request) {
        if (request == null || request.getLogicalBuIds() == null || request.getLogicalBuIds().isEmpty()) {
            throw new BadRequestException("logicalBuIds is required");
        }
        for (Long id : request.getLogicalBuIds()) {
            if (id == null || id <= 0) {
                throw new BadRequestException("invalid logicalBuId: " + id);
            }
        }
    }

    private LogicalBuOpResult internalError(long logicalBuId, Throwable th) {
        logger.error("[switchBM][{}] task failed", logicalBuId, th);
        LogicalBuOpResult result = new LogicalBuOpResult(logicalBuId);
        result.addFailure(OpFailure.console(OpStage.LOAD, OpErrorCode.INTERNAL_ERROR, null, LayeredCaller.rootMessage(th)));
        result.finish(LogicalBuOpResult.State.FAILED);
        return result;
    }

    // ---------------------------------------------------------------- load

    /** keepercontainers and dc names, loaded once and shared read-only by all BU tasks */
    @VisibleForTesting
    SharedContext loadShared() {
        SharedContext ctx = new SharedContext();
        try {
            // KeeperContainerService.findAll() (DAL "find-all") cross-joins DC_TBL without a join condition, so every
            // container comes back once per dc, and deleted containers are not filtered: dedupe by id, drop deleted.
            Map<Long, KeepercontainerTbl> unique = new LinkedHashMap<>();
            for (KeepercontainerTbl container : LayeredCaller.db(OpStage.LOAD, null, () -> keeperContainerService.findAll())) {
                if (!container.isDeleted()) {
                    unique.putIfAbsent(container.getKeepercontainerId(), container);
                }
            }
            ctx.containers.addAll(unique.values());
            List<DcTbl> dcs = LayeredCaller.db(OpStage.LOAD, null, () -> dcService.findAllDcs());
            if (dcs != null) {
                for (DcTbl dc : dcs) {
                    ctx.dcNames.put(dc.getId(), dc.getDcName());
                }
            }
        } catch (KeeperOpException e) {
            ctx.loadFailure = e.getFailure();
        }
        return ctx;
    }

    /** IDCs hosting this BU's TFS keepercontainers: the only IDCs whose keepers change priority. */
    private Set<String> idcsOf(long logicalBuId, SharedContext ctx) {
        Set<String> idcs = new TreeSet<>();
        for (KeepercontainerTbl container : ctx.containers) {
            if (container.getLogicalBuId() == logicalBuId
                    && KeeperDiskTypeUtils.isTfs(container.getKeepercontainerDiskType())) {
                String dc = ctx.dcNames.get(container.getKeepercontainerDc());
                if (dc != null) {
                    idcs.add(dc);
                }
            }
        }
        return idcs;
    }

    // ---------------------------------------------------------------- update DB

    /** Load BU + DB update. The BU lock is held until switchBM finished notifying, released there. */
    private Prepared prepare(long logicalBuId, Integer target, SharedContext ctx) {
        LogicalBuOpResult result = new LogicalBuOpResult(logicalBuId);
        if (!runningLogicalBus.add(logicalBuId)) {
            result.addFailure(OpFailure.console(OpStage.LOAD, OpErrorCode.OPERATION_CONFLICT, null, null));
            result.finish(LogicalBuOpResult.State.BLOCKED);
            return new Prepared(result, false);
        }
        OpStage stage = OpStage.LOAD;
        try {
            if (ctx.loadFailure != null) {
                result.addFailure(ctx.loadFailure);
                result.finish(LogicalBuOpResult.State.BLOCKED);
                return new Prepared(result, true);
            }
            LogicalBuModel logicalBu = loadLogicalBu(logicalBuId, result);
            if (logicalBu == null) {
                result.finish(LogicalBuOpResult.State.BLOCKED);
                return new Prepared(result, true);
            }
            result.setLogicalBuName(logicalBu.getName());
            result.setPriorityBefore(logicalBu.getPriority());
            result.getIdcs().addAll(idcsOf(logicalBuId, ctx));

            stage = OpStage.UPDATE_DB;
            if (!updatePriority(logicalBuId, logicalBu.getPriority(), target, result)) {
                result.finish(LogicalBuOpResult.State.FAILED);
                return new Prepared(result, true);
            }
            if (result.getIdcs().isEmpty()) {
                // no TFS keepercontainer: no keeper changes priority, nothing for MetaServer to reload
                result.finish(LogicalBuOpResult.State.SUCCESS);
                return new Prepared(result, true);
            }
            return new Prepared(result, true).markUpdated();
        } catch (Throwable th) {
            logger.error("[prepare][{}] unexpected failure at {}", logicalBuId, stage, th);
            result.addFailure(OpFailure.console(stage, OpErrorCode.INTERNAL_ERROR, null, LayeredCaller.rootMessage(th)));
            result.finish(stage == OpStage.LOAD ? LogicalBuOpResult.State.BLOCKED : LogicalBuOpResult.State.FAILED);
            return new Prepared(result, true);
        }
    }

    private LogicalBuModel loadLogicalBu(long logicalBuId, LogicalBuOpResult result) {
        try {
            LogicalBuModel logicalBu = LayeredCaller.db(OpStage.LOAD, null, () -> logicalBuService.findById(logicalBuId));
            if (logicalBu == null) {
                result.addFailure(OpFailure.console(OpStage.LOAD, OpErrorCode.LOGICAL_BU_NOT_FOUND, null, null));
            }
            return logicalBu;
        } catch (KeeperOpException e) {
            OpFailure failure = e.getFailure();
            if (failure.getCode() == OpErrorCode.BAD_REQUEST) {
                failure = OpFailure.console(OpStage.LOAD, OpErrorCode.LOGICAL_BU_NOT_FOUND, null, failure.getDetail());
            }
            result.addFailure(failure);
            return null;
        }
    }

    private boolean updatePriority(long logicalBuId, Integer expected, Integer target, LogicalBuOpResult result) {
        if (Objects.equals(expected, target)) {
            // idempotent: still reload, MetaServer may have missed the previous one
            result.setPriorityAfter(target);
            return true;
        }
        try {
            boolean updated = LayeredCaller.db(OpStage.UPDATE_DB, null,
                    () -> logicalBuService.casPriority(logicalBuId, expected, target));
            if (updated) {
                result.setPriorityAfter(target);
                return true;
            }
            Integer actual = rereadPriority(logicalBuId);
            result.setPriorityAfter(actual);
            if (Objects.equals(actual, target)) {
                return true;
            }
            result.addFailure(OpFailure.console(OpStage.UPDATE_DB, OpErrorCode.PRIORITY_STALE, null,
                    "expected=" + expected + ", actual=" + actual));
            return false;
        } catch (KeeperOpException e) {
            OpFailure failure = e.getFailure();
            if (failure.getCode() != OpErrorCode.DB_TIMEOUT) {
                // connect failure / SQL error: statement did not commit
                result.addFailure(failure);
                return false;
            }
            // timeout: the UPDATE may or may not have committed, read it back once
            try {
                Integer actual = rereadPriority(logicalBuId);
                result.setPriorityAfter(actual);
                if (Objects.equals(actual, target)) {
                    return true;
                }
                result.addFailure(failure);
            } catch (KeeperOpException reread) {
                result.addFailure(new OpFailure(OpLayer.CONSOLE_DB, OpStage.UPDATE_DB, OpErrorCode.DB_WRITE_UNKNOWN,
                        LayeredCaller.DB_TARGET, null, failure.getDetail()));
            }
            return false;
        }
    }

    private Integer rereadPriority(long logicalBuId) {
        return LayeredCaller.db(OpStage.UPDATE_DB, null, () -> logicalBuService.findById(logicalBuId).getPriority());
    }

    // ---------------------------------------------------------------- notify MetaServer

    /** @return dc → failure; IDCs that reloaded fine are absent */
    private Map<String, OpFailure> reloadDcs(Set<String> dcs) {
        Map<String, Future<OpFailure>> futures = new LinkedHashMap<>();
        for (String dc : dcs) {
            futures.put(dc, executor.submit(() -> reloadDc(dc)));
        }
        Map<String, OpFailure> failures = new HashMap<>();
        for (Map.Entry<String, Future<OpFailure>> entry : futures.entrySet()) {
            String dc = entry.getKey();
            OpFailure failure;
            try {
                failure = entry.getValue().get(RELOAD_WAIT_MILLI, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                entry.getValue().cancel(true);
                failure = new OpFailure(OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_TIMEOUT,
                        metaServerTarget(dc), dc, "reload exceeded " + RELOAD_WAIT_MILLI + "ms");
            } catch (ExecutionException e) {
                failure = toFailure(e.getCause(), dc);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failure = OpFailure.console(OpStage.NOTIFY_META, OpErrorCode.INTERNAL_ERROR, dc, "interrupted");
            }
            if (failure != null) {
                failures.put(dc, failure);
            }
        }
        return failures;
    }

    /** @return null when every MetaServer of the IDC applied (or had nothing to apply) */
    @VisibleForTesting
    OpFailure reloadDc(String dc) {
        String target = metaServerTarget(dc);
        List<DcMetaReloadResult> results;
        try {
            results = LayeredCaller.metaAt(OpStage.NOTIFY_META, target, dc,
                    () -> metaServerConsoleServiceManagerWrapper.get(dc).reloadDcMeta());
        } catch (KeeperOpException e) {
            HttpStatusCodeException http = LayeredCaller.find(e.getCause(), HttpStatusCodeException.class);
            if (http != null && http.getRawStatusCode() == 404) {
                // MetaServer not upgraded yet: DB already updated, its periodic refresh applies it
                return new OpFailure(OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_RELOAD_UNSUPPORTED,
                        target, dc, "HTTP 404 " + DcMetaReloadResult.PATH_RELOAD);
            }
            throw e;
        }
        if (results == null || results.isEmpty()) {
            return new OpFailure(OpLayer.META_SERVER, OpStage.NOTIFY_META, OpErrorCode.META_ERROR, target, dc,
                    "no MetaServer result");
        }
        List<DcMetaReloadResult> bad = new ArrayList<>();
        boolean anyFailed = false;
        for (DcMetaReloadResult r : results) {
            if (r.getStatus() == null || !r.getStatus().isSuccess()) {
                bad.add(r);
                anyFailed |= r.getStatus() == null || r.getStatus() == DcMetaReloadResult.Status.FAILED;
            }
        }
        if (bad.isEmpty()) {
            return null;
        }
        // the instances that did not apply, by their own address (the entry instance may be fine)
        List<String> badAddresses = new ArrayList<>();
        for (DcMetaReloadResult r : bad) {
            badAddresses.add(r.getAddress() == null ? "server_" + r.getServerId() : r.getAddress());
        }
        return new OpFailure(OpLayer.META_SERVER, OpStage.NOTIFY_META,
                anyFailed ? OpErrorCode.META_ERROR : OpErrorCode.META_RELOAD_NOT_APPLIED,
                String.join(",", badAddresses), dc, bad.toString());
    }

    private OpFailure toFailure(Throwable cause, String dc) {
        if (cause instanceof KeeperOpException) {
            return ((KeeperOpException) cause).getFailure();
        }
        return LayeredCaller.classifyMetaAt(OpStage.NOTIFY_META, metaServerTarget(dc), dc, cause);
    }

    /** MetaServer address configured for the dc (what console actually calls), e.g. 10.2.3.4:8080 or a domain. */
    @VisibleForTesting
    String metaServerTarget(String dc) {
        try {
            Map<String, String> metaservers = consoleConfig.getMetaservers();
            String address = metaservers == null ? null : metaservers.get(dc);
            if (address != null && !address.trim().isEmpty()) {
                return address.trim().replaceFirst("^https?://", "").replaceFirst("/+$", "");
            }
        } catch (Exception e) {
            logger.debug("[metaServerTarget][{}]", dc, e);
        }
        return "metaserver@" + dc;
    }

    // ---------------------------------------------------------------- model

    static class Prepared {
        final LogicalBuOpResult result;
        /** this task holds the BU lock and must release it */
        final boolean locked;
        /** priority is now the target in DB and the BU has IDCs to reload */
        boolean updated;

        Prepared(LogicalBuOpResult result, boolean locked) {
            this.result = result;
            this.locked = locked;
        }

        Prepared markUpdated() {
            this.updated = true;
            return this;
        }
    }

    /** Loaded once per switchBM request and shared read-only by all BU tasks. */
    static class SharedContext {
        final List<KeepercontainerTbl> containers = new ArrayList<>();
        final Map<Long, String> dcNames = new HashMap<>();
        /** keepercontainers / dc names could not be loaded: every BU of the request is blocked with this. */
        volatile OpFailure loadFailure;
    }
}
