package com.ctrip.xpipe.redis.keeper.container;


import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.ReplDelayConfigCache;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.health.DiskHealthChecker;
import com.ctrip.xpipe.redis.keeper.health.HealthState;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.store.TfsStoreDeleter;
import com.ctrip.xpipe.redis.keeper.store.searcher.CmdKeyItem;
import com.ctrip.xpipe.redis.keeper.store.searcher.GtidCommandSearcher;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class KeeperContainerService extends AbstractLifecycle implements TopElement, Observer {
	
	@Autowired
	KeeperConfig keeperConfig;
    @Autowired
    private LeaderElectorManager leaderElectorManager;
    @Autowired
    private KeeperContainerConfig keeperContainerConfig;
    @Autowired
    private KeepersMonitorManager keepersMonitorManager;
    @Autowired
    private KeeperResourceManager resourceManager;
    @Autowired
    private ContainerResourceManager containerResourceManager;
    @Autowired
    private GeneralRedisOpParser redisOpParser;
    @Autowired
    private DiskHealthChecker diskHealthChecker;
    @Autowired
    private SyncRateManager syncRateManager;
    @Autowired
    private ReplDelayConfigCache replDelayConfigCache;

    private Map<String, RedisKeeperServer> redisKeeperServers = Maps.newConcurrentMap();

    private ExecutorService searcherExecutor;

    private ExecutorService gcExecutor;

    private Logger logger = LoggerFactory.getLogger(KeeperContainerService.class);

    @Override
    protected void doInitialize() throws Exception {
        this.diskHealthChecker.addObserver(this);
        this.searcherExecutor = new ThreadPoolExecutor(1, 5, 1L, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(128), XpipeThreadFactory.create("keeper-searcher"));
        int gcParallelism = Math.max(1, keeperConfig.getOrphanStoreGcParallelism());
        this.gcExecutor = Executors.newFixedThreadPool(gcParallelism, XpipeThreadFactory.create("keeper-orphan-gc"));
    }

    @Override
    protected void doDispose() throws Exception {
        // process exit: registry has stopped every keeper, nothing writes; free stores before the FS goes down
        for (RedisKeeperServer keeperServer : redisKeeperServers.values()) {
            try {
                keeperServer.release();
            } catch (Throwable th) {
                logger.warn("[doDispose][release]{}", keeperServer, th);
            }
        }
        this.searcherExecutor.shutdown();
        this.gcExecutor.shutdown();
        containerResourceManager.shutdownAsyncFileSystem();
    }

    @Override
    public void update(Object args, Observable observable) {
        if (args instanceof HealthState && !((HealthState) args).isUp()) {
            long currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            // reset leader
            for (RedisKeeperServer keeper: redisKeeperServers.values()) {
                if (keeper.isLeader()) {
                    long lastResetTime = keeper.getLastElectionResetTime();
                    if (lastResetTime <= currentTime
                            && currentTime - lastResetTime < keeperContainerConfig.keeperLeaderResetMinInterval()) {
                        logger.debug("[container_unhealthy][{}] last reset at: {}, skip", keeper.getReplId(), lastResetTime);
                        continue;
                    }

                    logger.info("[container_unhealthy][{}] reset leader", keeper.getReplId());
                    keeper.resetElection();
                }
            }
        }
    }

    public RedisKeeperServer add(KeeperTransMeta keeperTransMeta) {
        KeeperMeta keeperMeta = keeperTransMeta.getKeeperMeta();
        fillContainerKeeperIp(keeperMeta);
        enrichKeeperMetaFromKeeperTransMeta(keeperMeta, keeperTransMeta);

        String keeperServerKey = assembleKeeperServerKey(keeperTransMeta);
        if (!redisKeeperServers.containsKey(keeperServerKey)) {
            synchronized (this) {
                if (!redisKeeperServers.containsKey(keeperServerKey)) {
                    if (!containerResourceManager.applyPort(keeperMeta.getPort())) {
                        throw new RedisKeeperRuntimeException(
                                new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_EXIST,
                                        String.format("Add keeper for cluster %d shard %d failed since port %d is already used",
                                                keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId(), keeperMeta.getPort())),
                                null);
                    }

                    File baseDir = getReplicationStoreDir(keeperMeta);
                    DefaultRedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeperTransMeta.getReplId(), keeperMeta,
                            keeperConfig, baseDir, leaderElectorManager, keepersMonitorManager, resourceManager, syncRateManager,
                            redisOpParser, containerResourceManager.getAsyncFileSystem(), replDelayConfigCache,
                            keeperContainerConfig.isTfsMode());

                    try {
                        register(redisKeeperServer);
                        redisKeeperServers.put(keeperServerKey, redisKeeperServer);
                        return redisKeeperServer;
                    } catch (Throwable regTh) {
                        logger.info("[add] register fail", regTh);
                        try {
                            deRegister(redisKeeperServer);
                        } catch (Throwable deregTh) {
                            logger.info("[add][rollback] deregister fail", deregTh);
                        }
                        if (!redisKeeperServer.getLifecycleState().isDisposed()) {
                            try {
                                LifecycleHelper.stopIfPossible(redisKeeperServer);
                                LifecycleHelper.disposeIfPossible(redisKeeperServer);
                            } catch (Throwable th) {
                                logger.info("[add][rollback] dispose fail", th);
                            }
                        }
                        try {
                            // stop/dispose no longer free the store
                            redisKeeperServer.release();
                        } catch (Throwable th) {
                            logger.info("[add][rollback] release fail", th);
                        }
                        if (!redisKeeperServers.containsKey(keeperServerKey)) {
                            containerResourceManager.releasePort(keeperMeta.getPort());
                        }

                        throw new RedisKeeperRuntimeException(
                                new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                                        String.format("Add keeper for cluster %d shard %d failed",
                                                keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId())), regTh);
                    }
                }
            }
        }

        throw new RedisKeeperRuntimeException(
                new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_EXIST,
                        String.format("Keeper already exists for cluster %d shard %d",
                                keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId())), null);
    }

    public CommandFuture<List<CmdKeyItem>> searchKeeperCmdKeys(ReplId replId, String uuid, long begGno, long endGno) {
        RedisKeeperServer redisKeeperServer = redisKeeperServers.get(replId.toString());
        if (null == redisKeeperServer) {
            throw new RedisKeeperRuntimeException("unfound keeper " + replId);
        } else if (!redisKeeperServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException("keeper state " + redisKeeperServer.getLifecycleState().getPhaseName());
        }

        GtidCommandSearcher searcher = redisKeeperServer.createCmdKeySearcher(uuid, begGno, endGno);
        return searcher.execute(searcherExecutor);
    }

    public RedisKeeperServer addOrStart(KeeperTransMeta keeperTransMeta) {
        String keeperServerKey = assembleKeeperServerKey(keeperTransMeta);
        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);
        if (keeperServer == null) {
            return add(keeperTransMeta);
        }

        start(ReplId.from(keeperTransMeta.getReplId()));

        return keeperServer;
    }

    public List<RedisKeeperServer> list() {
        return Lists.newArrayList(redisKeeperServers.values());
    }

    public KeeperDiskInfo infoDisk() {
        return diskHealthChecker.getResult();
    }

    public void resetElection(ReplId replId) {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Reset election for %s failed since keeper doesn't exist", replId)), null);
        }

        if (!keeperServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_STARTED,
                            String.format("Keeper for %s has not started", replId)), null);
        }

        keeperServer.resetElection();
    }

    public void releaseRdb(ReplId replId) throws IOException {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Release rdb for %s failed since keeper doesn't exist", replId)), null);
        }

        if (!keeperServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_STARTED,
                            String.format("Keeper for %s has not started", replId)), null);
        }

        keeperServer.releaseRdb();
    }

    public void start(ReplId replId) {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Start keeper for %s failed since keeper doesn't exist", replId)), null);
        }

        if (keeperServer.getLifecycleState().isStarted()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_STARTED,
                            String.format("Keeper for %s already started", replId)), null);
        }

        try {
            keeperServer.start();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Start keeper failed for %s", replId)), ex);
        }
    }

    public void stop(ReplId replId) {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_NOT_EXIST,
                            String.format("Stop keeper for %s failed since keeper doesn't exist", replId)), null);
        }

        if (keeperServer.getLifecycleState().isStopped()) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.KEEPER_ALREADY_STOPPED,
                            String.format("Keeper for %s already stopped", replId)), null);
        }

        try {
            keeperServer.stop();
            // stop no longer frees the store: hand the TFS lease back; /start reopens lazily
            keeperServer.release();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Stop keeper failed for %s", replId)), ex);
        }
    }

    public void remove(ReplId replId) {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            return;
        }

        try {
            // 1. stop and dispose keeper server
            deRegister(keeperServer);

            // 2. remove keeper
            removeKeeperCache(keeperServerKey);

            // 3. clean external resources, such as replication stores
            keeperServer.destroy();

        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Remove keeper failed for %s", replId)), ex);
        }
    }


    /**
     * Like {@link #remove(ReplId)}, but never deletes the replication store — for a shared TFS store
     * that must outlive this keeper instance (e.g. migration, non-holder keepers).
     */
    public void release(ReplId replId) {
        String keeperServerKey = replId.toString();

        RedisKeeperServer keeperServer = redisKeeperServers.get(keeperServerKey);

        if (keeperServer == null) {
            return;
        }

        try {
            // 1. stop and dispose keeper server
            deRegister(keeperServer);

            // 2. remove keeper
            removeKeeperCache(keeperServerKey);

            // 3. close external resources without deleting them
            keeperServer.release();
        } catch (Throwable ex) {
            throw new RedisKeeperRuntimeException(
                    new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                            String.format("Release keeper failed for %s", replId)), ex);
        }
    }

    /**
     * Entry names under the TFS store root; the caller keeps only {@code repl_<id>}. Throws when the
     * root can't be listed (or this container isn't TFS), so the caller retries another container
     * instead of reading it as an empty mount.
     */
    public List<String> listStoreDirNames() {
        if (!keeperContainerConfig.isTfsMode()) {
            throw new RedisKeeperRuntimeException(new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                    "not tfs mode, no shared store root"), null);
        }
        String root = StringUtils.trimTrailingCharacter(keeperContainerConfig.getReplicationStoreDir(), '/');
        AsyncFileSystem fs = containerResourceManager.getAsyncFileSystem();
        try {
            List<String> names = AsyncFileSystemHelper.await(() -> fs.list(root), "gc list store root " + root);
            return names == null ? Collections.emptyList() : names;
        } catch (IOException e) {
            throw new RedisKeeperRuntimeException(new ErrorMessage<>(KeeperContainerErrorCode.INTERNAL_EXCEPTION,
                    "list store root failed: " + root), e);
        }
    }

    /**
     * Fallback GC for a TFS store with no live keeper on this container; the delete itself is the same
     * {@link TfsStoreDeleter} destroy uses, with the orphan idle threshold.
     */
    public GcOrphanStoreResult gcOrphanStore(Long replId) {
        if (!keeperContainerConfig.isTfsMode()) {
            return GcOrphanStoreResult.skip(replId, "not tfs mode");
        }
        if (redisKeeperServers.containsKey(ReplId.from(replId).toString())) {
            return GcOrphanStoreResult.skip(replId, "live keeper on this container");
        }
        File baseDir = new File(
                StringUtils.trimTrailingCharacter(keeperContainerConfig.getReplicationStoreDir(), '/'),
                ReplId.from(replId).toString());
        GcOrphanStoreResult result = new TfsStoreDeleter(containerResourceManager.getAsyncFileSystem(),
                keeperConfig.getOrphanStoreMinIdleMilliToGc()).delete(baseDir, ReplId.from(replId));
        logger.info("[gcOrphanStore]{}", result);
        return result;
    }

    /**
     * Batches {@link #gcOrphanStore(Long)} over the transport <b>and</b> runs each replId's
     * safety-check-then-delete concurrently on {@link #gcExecutor} — one request/response for a batch
     * that can have dozens to low hundreds of candidates, with their IO waits overlapping instead of
     * serialized. Every replId's checks and file ops are independent of every other's (different
     * directories, different meta files), so running them concurrently changes nothing about what gets
     * deleted — only how long the whole batch takes wall-clock. {@code gcExecutor} is a small, fixed
     * pool (see {@code keeperConfig.getOrphanStoreGcParallelism}): this is a background task that must
     * never meaningfully compete with normal replication traffic for TFS IOPS, so concurrency here is
     * capped on purpose rather than fanned out to match the batch size.
     * <p>
     * Results line up with {@code replIds} by index, not completion order, so callers never have to
     * care that this ran concurrently.
     */
    public List<GcOrphanStoreResult> gcOrphanStores(List<Long> replIds) {
        List<Future<GcOrphanStoreResult>> futures = new ArrayList<>(replIds.size());
        for (Long replId : replIds) {
            futures.add(gcExecutor.submit(() -> gcOrphanStore(replId)));
        }

        List<GcOrphanStoreResult> results = new ArrayList<>(replIds.size());
        for (int i = 0; i < futures.size(); i++) {
            Long replId = replIds.get(i);
            try {
                results.add(futures.get(i).get());
            } catch (Throwable th) {
                // gcOrphanStore itself never throws; this is only reachable via executor-level failure
                // (rejection, interruption) — never silently drop a candidate from the result list.
                logger.error("[gcOrphanStores][{}] gc task failed", replId, th);
                results.add(GcOrphanStoreResult.fail(replId, th.getMessage()));
            }
        }
        return results;
    }

    private void removeKeeperCache(String keeperServerKey) {
        RedisKeeperServer redisKeeperServer = redisKeeperServers.remove(keeperServerKey);
        if (redisKeeperServer != null) {
            containerResourceManager.releasePort(redisKeeperServer.getListeningPort());
        }
    }

    private void fillContainerKeeperIp(KeeperMeta keeperMeta) {
        if (!StringUtils.hasText(keeperMeta.getHostname())) {
            return;
        }

        String localIp = getLocalIp();
        if (!StringUtils.hasText(localIp) || "0.0.0.0".equals(localIp)) {
            throw new RedisKeeperRuntimeException("invalid local ip for container keeper: " + localIp
                    + ", hostname: " + keeperMeta.getHostname());
        }
        keeperMeta.setIp(localIp);
    }

    @VisibleForTesting
    protected String getLocalIp() {
        return FoundationService.DEFAULT.getLocalIp();
    }

    private RedisKeeperServer doAdd(KeeperTransMeta keeperTransMeta, KeeperMeta keeperMeta) throws Exception {

        File baseDir = getReplicationStoreDir(keeperMeta);
        return createRedisKeeperServer(keeperTransMeta.getReplId(), keeperMeta, baseDir);
    }

    private void enrichKeeperMetaFromKeeperTransMeta(KeeperMeta keeperMeta, KeeperTransMeta keeperTransMeta) {
        ClusterMeta clusterMeta = new ClusterMeta().setDbId(keeperTransMeta.getClusterDbId());
        ShardMeta shardMeta = new ShardMeta().setDbId(keeperTransMeta.getShardDbId());
        shardMeta.setParent(clusterMeta);
        keeperMeta.setParent(shardMeta);
    }

    private RedisKeeperServer createRedisKeeperServer(Long replId, KeeperMeta keeper,
                                                      File baseDir) throws Exception {

        DefaultRedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(replId, keeper, keeperConfig,
                baseDir, leaderElectorManager, keepersMonitorManager, resourceManager, syncRateManager,
                redisOpParser, containerResourceManager.getAsyncFileSystem(), replDelayConfigCache,
                keeperContainerConfig.isTfsMode());

        register(redisKeeperServer);
        return redisKeeperServer;
    }

    private void register(RedisKeeperServer redisKeeperServer) throws Exception {
        ComponentRegistryHolder.getComponentRegistry().add(redisKeeperServer);
    }

    private void deRegister(RedisKeeperServer redisKeeperServer) throws Exception {
        ComponentRegistryHolder.getComponentRegistry().remove(redisKeeperServer);
    }

    private File getReplicationStoreDir(KeeperMeta keeperMeta) {
        String baseDir = keeperContainerConfig.getReplicationStoreDir();
        baseDir = StringUtils.trimTrailingCharacter(baseDir, '/');
        if (keeperContainerConfig.isTfsMode()) {
            return new File(baseDir);
        }
        return new File(String.format("%s/replication_store_%s", baseDir, keeperMeta.getPort()));
    }

    private String assembleKeeperServerKey(KeeperTransMeta keeperTransMeta) {
        return ReplId.from(keeperTransMeta.getReplId()).toString();
    }

    @VisibleForTesting
    protected void setRedisKeeperServers(Map<String, RedisKeeperServer> servers) {
        this.redisKeeperServers = servers;
    }

}
