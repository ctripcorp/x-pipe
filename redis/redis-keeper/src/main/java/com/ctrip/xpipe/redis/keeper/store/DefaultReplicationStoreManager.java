package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.util.KeeperReplIdAwareThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author marsqing
 * <p>
 * May 31, 2016 5:33:46 PM
 */
public class DefaultReplicationStoreManager extends AbstractLifecycleObservable implements ReplicationStoreManager {

    private final static String META_FILE = "store_manager_meta.properties";

    private static final String LATEST_STORE_DIR = "latest.store.dir";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ReplId replId;

    private final String keeperRunid;

    private final File keeperBaseDir;

    private File baseDir;

    private File metaFile;

    private final AtomicReference<Properties> currentMeta = new AtomicReference<Properties>();

    private final AtomicReference<ReplicationStore> currentStore = new AtomicReference<>();

    private final KeeperConfig keeperConfig;

    private final AtomicLong gcCount = new AtomicLong();

    private ScheduledFuture<?> gcFuture;

    private ScheduledExecutorService scheduled;

    private final KeeperMonitor keeperMonitor;

    private final RedisOpParser redisOpParser;

    private SyncRateManager syncRateManager;

    private CKStore ckStore;

    private final AsyncFileSystem asyncFileSystem;

    private final ScheduledExecutorService commandNotifyScheduler;

    /** Long-lived handle for {@link #META_FILE}; lazy open, closed on stop/dispose (Phase S / T-S.5). */
    private AsyncFile managerMetaAsyncFile;

    /**
     * Permanent gate after {@link #destroy()}: unlike stop (lazy reopen on start), destroy must never
     * reopen manager-meta (avoids open between close and rmdir, or after baseDir gone).
     */
    private boolean managerMetaDestroyed;

    public DefaultReplicationStoreManager(KeeperConfig keeperConfig, ReplId replId,
                                          String keeperRunid, File baseDir, KeeperMonitor keeperMonitor,
                                          SyncRateManager syncRateManager, RedisOpParser redisOpParser,
                                          ScheduledExecutorService commandNotifyScheduler,
                                          AsyncFileSystem asyncFileSystem) {
        super(MoreExecutors.directExecutor());
        this.replId = replId;
        this.keeperRunid = keeperRunid;
        this.keeperConfig = keeperConfig;
        this.keeperMonitor = keeperMonitor;
        this.keeperBaseDir = baseDir;
        this.redisOpParser = redisOpParser;
        this.syncRateManager = syncRateManager;
        this.commandNotifyScheduler = commandNotifyScheduler;
        this.asyncFileSystem = Objects.requireNonNull(asyncFileSystem, "asyncFileSystem");
    }

    public DefaultReplicationStoreManager(CKStore ckStore, KeeperConfig keeperConfig, ReplId replId,
                                          String keeperRunid, File baseDir, KeeperMonitor keeperMonitor,
                                          SyncRateManager syncRateManager, RedisOpParser redisOpParser,
                                          ScheduledExecutorService commandNotifyScheduler,
                                          AsyncFileSystem asyncFileSystem) {
        this(keeperConfig, replId, keeperRunid, baseDir, keeperMonitor, syncRateManager, redisOpParser, commandNotifyScheduler, asyncFileSystem);
        this.ckStore = ckStore;
    }

    @Override
    protected void doInitialize() throws Exception {

        this.baseDir = new File(keeperBaseDir, replId.toString());
        this.metaFile = new File(this.baseDir, META_FILE);

        scheduled = Executors.newScheduledThreadPool(1,
                KeeperReplIdAwareThreadFactory.create(replId.toString(), "gc-" + replId.toString()));
    }

    /**
     * Start Manager GC. PREPARE → ACTIVE/BACKUP (Phase Rc) re-enters via {@code start()} again.
     */
    @Override
    protected void doStart() throws Exception {
        gcFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                gc();
            }
        }, keeperConfig.getReplicationStoreGcIntervalSeconds(), keeperConfig.getReplicationStoreGcIntervalSeconds(), TimeUnit.SECONDS);
    }

    /**
     * PREPARE / Server stop: cancel GC → best-effort flush → close store handles (not destroy)
     * → close manager-meta long-lived handle (reopen lazily after start).
     * <p>
     * {@code releaseCurrentStore} failures are swallowed (align {@link #doDispose}): prefer reaching
     * Lifecycle STOPPED over propagating — {@link com.ctrip.xpipe.lifecycle.AbstractLifecycle#stop}
     * rollback would leave STARTED with GC/store/meta already torn down. Manager-meta close is in
     * {@code finally}. TODO: surface unclosed FS handles to ForceCloseDir when close semantics are clear.
     */
    @Override
    protected void doStop() throws Exception {
        cancelGcFuture();
        flushStoreBestEffort();
        try {
            releaseCurrentStore();
        } catch (Exception e) {
            logger.warn("[doStop][releaseCurrentStore]", e);
        } finally {
            closeManagerMetaFile();
        }
    }

    @Override
    protected void doDispose() throws Exception {
        cancelGcFuture();
        try {
            releaseCurrentStore();
        } catch (Exception e) {
            logger.warn("[doDispose][releaseCurrentStore]", e);
        } finally {
            closeManagerMetaFile();
        }
        if (scheduled != null) {
            scheduled.shutdownNow();
        }
    }

    private void cancelGcFuture() {
        if (gcFuture != null) {
            gcFuture.cancel(true);
            gcFuture = null;
        }
    }

    /**
     * Best-effort cmd sliding-window / index flush before close. Failures are WARN-only
     * (lease release preferred over perfect durability; ForceCloseDir as fallback).
     */
    private void flushStoreBestEffort() {
        try {
            ReplicationStore store = currentStore.get();
            if (store == null) {
                return;
            }
            store.flushPendingData();
        } catch (Exception e) {
            logger.warn("[doStop][flush best-effort failed]{}", this, e);
        }
    }

    @Override
    public synchronized void releaseCurrentStore() throws IOException {
        logger.info("[releaseCurrentStore]{}", this);
        ReplicationStore replicationStore = currentStore.get();
        if (replicationStore == null) {
            return;
        }
        try {
            replicationStore.close();
        } finally {
            // Always drop the lease reference so PREPARE cannot reopen via getCurrent.
            currentStore.set(null);
        }
    }

    @Override
    public synchronized ReplicationStore createIfNotExist() throws IOException {

        // STOPPING / stop / dispose: refuse reopen / self-heal. Initialized-but-never-started still allowed
        // (isPositivelyStopped distinguishes Stoppable.PHASE_NAME_END from Initializable.PHASE_NAME_END).
        if (refuseOpenOrCreate()) {
            ReplicationStore existing = currentStore.get();
            if (existing != null && existing.checkOk()) {
                return existing;
            }
            throw new IOException("replication store manager stopped, refuse createIfNotExist: " + this);
        }

        // Heal only when getCurrent() == null (no store / !checkOk). IO / unexpected failures must propagate
        // — never swallow then create() a new UUID (Important #3 / T-S.13).
        ReplicationStore currentReplicationStore = getCurrent();
        if (currentReplicationStore == null) {
            logger.info("[createIfNotExist]{}", baseDir);
            currentReplicationStore = create();
        }
        return currentReplicationStore;
    }

    @Override
    public synchronized ReplicationStore create() throws IOException {

        if (!getLifecycleState().isInitialized()) {
            throw new ReplicationStoreManagerStateException("can not create", toString(), getLifecycleState().getPhaseName());
        }
        if (refuseOpenOrCreate()) {
            throw new IOException("replication store manager stopped, refuse create: " + this);
        }

        keeperMonitor.getReplicationStoreStats().increateReplicationStoreCreateCount();

        File storeBaseDir = new File(baseDir, UUID.randomUUID().toString());
        AsyncFileSystemHelper.await(() -> asyncFileSystem.mkdir(storeBaseDir.getAbsolutePath(), true),
                "mkdir replication store " + storeBaseDir.getAbsolutePath());

        logger.info("[create]{}", storeBaseDir);

        // Construct before publishing latest: avoid latest.store.dir pointing at an empty UUID dir
        // when createReplicationStore fails (GC could then reclaim the still-serving old store).
        ReplicationStore replicationStore = createReplicationStore(storeBaseDir, keeperConfig, keeperRunid, keeperMonitor, syncRateManager);
        try {
            recordLatestStore(storeBaseDir.getName());
        } catch (Exception e) {
            // Meta not published — close the unpublished store; keep old lease / latest unchanged.
            // Catch Exception: FS may throw sync RuntimeException beyond IOException (T-S.14).
            try {
                replicationStore.close();
            } catch (Exception closeErr) {
                logger.warn("[create][close unpublished store after meta fail]{}", storeBaseDir, closeErr);
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("record latest store failed: " + storeBaseDir, e);
        }

        try {
            releaseCurrentStore();
        } catch (IOException e) {
            logger.info("[create][release previous store]", e);
        }

        currentStore.set(replicationStore);

        notifyObservers(new NodeAdded<ReplicationStore>(replicationStore));
        return currentStore.get();
    }

    /** STOPPING window or after stop/dispose — refuse reopen / create / self-heal (T-S.12). */
    private boolean refuseOpenOrCreate() {
        return getLifecycleState().isStopping() || getLifecycleState().isPositivelyStopped();
    }

    protected ReplicationStore createReplicationStore(File storeBaseDir, KeeperConfig keeperConfig, String keeperRunid,
                                                      KeeperMonitor keeperMonitor, SyncRateManager syncRateManager) throws IOException {
        return new GtidReplicationStore(this.ckStore,storeBaseDir,keeperConfig,keeperRunid, keeperMonitor, redisOpParser,
                syncRateManager, commandNotifyScheduler, asyncFileSystem, replId);
    }

    private void recordLatestStore(String storeDir) throws IOException {
        Properties meta = currentMeta();
        meta.setProperty(LATEST_STORE_DIR, storeDir);
        saveMeta(meta);
    }

    /**
     * @param meta
     * @throws IOException
     */
    private void saveMeta(Properties meta) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        meta.store(out, null);
        byte[] data = out.toByteArray();

        AsyncFile asyncFile = getOrOpenManagerMetaFile();
        AsyncFileSystemHelper.writeAllBytes(asyncFileSystem, asyncFile, data,
                "write manager meta " + metaFile.getAbsolutePath());

        logger.info("[saveMeta][before]{}", currentMeta.get());
        currentMeta.set(meta);
        logger.info("[saveMeta][after]{}", currentMeta.get());
    }

    /**
     * @return never null; empty {@link Properties} when file absent / size 0
     * @throws IOException
     */
    private Properties loadMeta() throws IOException {
        AsyncFile asyncFile = getOrOpenManagerMetaFile();
        long size = AsyncFileSystemHelper.await(() -> asyncFileSystem.size(asyncFile),
                "stat manager meta " + metaFile.getAbsolutePath());
        if (size > Integer.MAX_VALUE) {
            throw new IOException("async file too large: " + metaFile.getAbsolutePath());
        }
        if (size == 0) {
            return new Properties();
        }
        Properties meta = new Properties();
        byte[] data = AsyncFileSystemHelper.readAllBytes(asyncFileSystem, asyncFile, size, 0,
                "read manager meta " + metaFile.getAbsolutePath());
        try (InputStream in = new ByteArrayInputStream(data)) {
            meta.load(in);
        }
        return meta;
    }

    /**
     * Sole entry for manager-meta handle. Synchronized + lifecycle / destroy gate: refuse open/use while
     * stopping or after stop/dispose (handle closed in {@link #closeManagerMetaFile()}), and after
     * {@link #destroy()} (permanent). After {@code start()} again (not destroyed), first save/load reopens lazily.
     */
    private synchronized AsyncFile getOrOpenManagerMetaFile() throws IOException {
        if (managerMetaDestroyed) {
            throw new IOException("replication store manager destroyed, refuse manager meta: " + this);
        }
        if (getLifecycleState().isStopping() || getLifecycleState().isPositivelyStopped()) {
            throw new IOException("replication store manager stopped, refuse manager meta: " + this);
        }
        if (managerMetaAsyncFile != null) {
            return managerMetaAsyncFile;
        }
        // Parent dir may not exist before first create(); open(CREATE) needs it.
        AsyncFileSystemHelper.await(() -> asyncFileSystem.mkdir(baseDir.getAbsolutePath(), true),
                "mkdir manager baseDir for meta " + baseDir.getAbsolutePath());
        AsyncFile asyncFile = AsyncFileSystemHelper.awaitOpen(asyncFileSystem, () -> asyncFileSystem.open(metaFile.getAbsolutePath(), AbstractStorageFile.OpenMode.READ_WRITE, true, true,
                        replId.toString()),
                "open manager meta " + metaFile.getAbsolutePath());
        managerMetaAsyncFile = asyncFile;
        return managerMetaAsyncFile;
    }

    /**
     * Idempotent close of manager-meta handle. Decoupled from {@link #releaseCurrentStore()};
     * called from stop/dispose/destroy. Must not reopen until lifecycle leaves stopped
     * ({@link #getOrOpenManagerMetaFile()} gated).
     */
    private synchronized void closeManagerMetaFile() {
        if (managerMetaAsyncFile == null) {
            return;
        }
        AsyncFile toClose = managerMetaAsyncFile;
        managerMetaAsyncFile = null;
        AsyncFileSystemHelper.closeHandle(asyncFileSystem, toClose,
                "close manager meta " + metaFile.getAbsolutePath());
    }

    private Properties currentMeta() throws IOException {

        return currentMeta(false);
    }

    private Properties currentMeta(boolean forceLoad) throws IOException {

        if (forceLoad || currentMeta.get() == null) {
            currentMeta.set(loadMeta());
        }
        return currentMeta.get();
    }

    @Override
    public synchronized ReplicationStore getCurrent() throws IOException {

        if (currentStore.get() == null) {
            if (refuseOpenOrCreate()) {
                logger.info("[getCurrent][stopping/stopped][skip reopen]{}", this);
                return null;
            }
            Properties meta = currentMeta();
            if (meta != null) {
                if (meta.getProperty(LATEST_STORE_DIR) != null) {
                    File latestStoreDir = new File(baseDir, meta.getProperty(LATEST_STORE_DIR));
                    logger.info("[getCurrent][latest]{}", latestStoreDir);
                    if (AsyncFileSystemHelper.await(() -> asyncFileSystem.exists(latestStoreDir.getAbsolutePath()),
                            "check latest store dir exists " + latestStoreDir.getAbsolutePath())) {
                        currentStore.set(createReplicationStore(latestStoreDir, keeperConfig, keeperRunid, keeperMonitor, syncRateManager));
                    }
                }
            }
        }

        ReplicationStore replicationStore = currentStore.get();
        if (replicationStore != null && !replicationStore.checkOk()) {
            // Escape hatch only: do not clear currentStore here. Lease release must go through
            // releaseCurrentStore() (e.g. create() / Manager.stop); checkOk may mean more than closed later.
            logger.info("[getCurrent][store not ok, return null]{}", replicationStore);
            return null;
        }
        return currentStore.get();
    }

    @Override
    public ReplId getReplId() {
        return replId;
    }

    protected synchronized void gc() throws IOException {

        logger.debug("[gc]{}", this);

        if (!getLifecycleState().isStarted()) {
            logger.info("[gc][not started][skip]{}", this);
            return;
        }

        gcCount.incrementAndGet();
        Properties meta = currentMeta(true);
        if (meta != null) {
            final String currentDirName = meta.getProperty(LATEST_STORE_DIR);
            List<String> children = AsyncFileSystemHelper.await(() -> asyncFileSystem.list(baseDir.getAbsolutePath()),
                    "list replication store manager baseDir " + baseDir);

            if (children != null && !children.isEmpty()) {

                logger.info("[GC][old replicationstore]newest:{}", currentDirName);
                for (String name : children) {
                    if (currentDirName != null && currentDirName.equals(name)) {
                        continue;
                    }
                    String childPath = new File(baseDir, name).getAbsolutePath();
                    boolean isDir = AsyncFileSystemHelper.await(() -> asyncFileSystem.isDirectory(childPath),
                            "isDirectory " + childPath);
                    if (!isDir) {
                        continue;
                    }
                    // TODO T-FS.14: replace with asyncFileSystem.lastModified(childPath) once path-level mtime lands.
                    long lastModified = new File(childPath).lastModified();
                    if (System.currentTimeMillis() - lastModified > keeperConfig.getReplicationStoreMinTimeMilliToGcAfterCreate()) {
                        logger.info("[GC] directory {}", childPath);
                        AsyncFileSystemHelper.await(() -> asyncFileSystem.rmdir(childPath, true),
                                "rmdir " + childPath);
                    } else {
                        logger.warn("[GC][directory is created too short, do not gc]{}, {}", childPath, new Date(lastModified));
                    }
                }
            }
        }

        // gc current ReplicationStore
        ReplicationStore replicationStore = getCurrent();
        if (replicationStore != null) {
            replicationStore.gc();
        }
    }

    @Override
    public synchronized void destroy() throws Exception {
        logger.info("[destroy]{}", this);
        // Permanent refuse reopen before close/rmdir so create/getCurrent/gc cannot race a new open.
        managerMetaDestroyed = true;
        closeManagerMetaFile();
        AsyncFileSystemHelper.await(() -> asyncFileSystem.rmdir(this.baseDir.getAbsolutePath(), true),
                "rmdir replication store manager baseDir " + baseDir);
    }

    public long getGcCount() {
        return gcCount.get();
    }

    @Override
    public String toString() {
        return String.format("repl:%s, keeperRunId:%s, baseDir:%s, currentMeta:%s", replId, keeperRunid, baseDir,
                currentMeta.get() == null ? "" : currentMeta.get().toString());
    }

    public File getBaseDir() {
        return baseDir;
    }

}
