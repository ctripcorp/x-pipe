package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.pubsub.KeeperPubSubParseHook;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.util.KeeperReplIdAwareThreadFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.nio.NioEventLoopGroup;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

    static final String READ_ONLY_STORE_MSG = "read only store";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ReplId replId;

    private final String keeperRunid;

    private final File keeperBaseDir;

    private File baseDir;

    private File metaFile;

    private final AtomicReference<Properties> currentMeta = new AtomicReference<Properties>();

    private final AtomicReference<ReplicationStore> currentStore = new AtomicReference<>();

    private volatile boolean readOnly;

    private volatile KeeperPubSubParseHook pubSubParseHook;

    private final KeeperConfig keeperConfig;

    private final AtomicLong gcCount = new AtomicLong();

    private ScheduledFuture<?> gcFuture;

    static final long META_FLUSH_CHECK_PERIOD_MILLI = 10_000L;

    private volatile long metaFlushCheckPeriodMilli = META_FLUSH_CHECK_PERIOD_MILLI;

    private volatile ScheduledFuture<?> metaFlushFuture;

    private volatile long lastMetaFlushMilli;

    /** Directory name of {@link #currentStore}; published / cleared together with it under the monitor. */
    private String currentStoreDirName;

    private ScheduledExecutorService scheduled;

    private final KeeperMonitor keeperMonitor;

    private final RedisOpParser redisOpParser;

    private SyncRateManager syncRateManager;

    private CKStore ckStore;

    private NioEventLoopGroup masterEventLoopGroup;

    private final AsyncFileSystem asyncFileSystem;

    /** Long-lived handle for {@link #META_FILE}; lazy open, closed on stop/dispose (Phase S / T-S.5). */
    private AsyncFile managerMetaAsyncFile;

    private final AtomicBoolean stopReleaseFailed = new AtomicBoolean();

    /**
     * Permanent gate after {@link #destroy()}: unlike stop (lazy reopen on start), destroy must never
     * reopen manager-meta (avoids open between close and rmdir, or after baseDir gone).
     */
    private boolean managerMetaDestroyed;

    public DefaultReplicationStoreManager(KeeperConfig keeperConfig, ReplId replId,
                                          String keeperRunid, File baseDir, KeeperMonitor keeperMonitor,
                                          SyncRateManager syncRateManager, RedisOpParser redisOpParser,
                                          AsyncFileSystem asyncFileSystem) {
        super(MoreExecutors.directExecutor());
        this.replId = replId;
        this.keeperRunid = keeperRunid;
        this.keeperConfig = keeperConfig;
        this.keeperMonitor = keeperMonitor;
        this.keeperBaseDir = baseDir;
        this.redisOpParser = redisOpParser;
        this.syncRateManager = syncRateManager;
        this.asyncFileSystem = Objects.requireNonNull(asyncFileSystem, "asyncFileSystem");
    }

    public DefaultReplicationStoreManager(CKStore ckStore, NioEventLoopGroup masterEventLoopGroup, KeeperConfig keeperConfig, ReplId replId,
                                          String keeperRunid, File baseDir, KeeperMonitor keeperMonitor,
                                          SyncRateManager syncRateManager, RedisOpParser redisOpParser,
                                          AsyncFileSystem asyncFileSystem) {
        this(keeperConfig, replId, keeperRunid, baseDir, keeperMonitor, syncRateManager, redisOpParser, asyncFileSystem);
        this.ckStore = ckStore;
        this.masterEventLoopGroup = masterEventLoopGroup;
    }

    @Override
    protected void doInitialize() throws Exception {

        this.baseDir = new File(keeperBaseDir, replId.toString());
        this.metaFile = new File(this.baseDir, META_FILE);

        scheduled = Executors.newScheduledThreadPool(1,
                KeeperReplIdAwareThreadFactory.create(replId.toString(), "gc-" + replId.toString()));
    }

    /**
     * Start Manager GC and meta flush. PREPARE → ACTIVE/BACKUP (Phase Rc) re-enters via {@code start()} again.
     * Read-only mode skips both (D7b).
     */
    @Override
    protected void doStart() throws Exception {
        if (readOnly) {
            logger.info("[doStart][readOnly][skip gc and meta flush]{}", this);
            return;
        }
        gcFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                gc();
            }
        }, keeperConfig.getReplicationStoreGcIntervalSeconds(), keeperConfig.getReplicationStoreGcIntervalSeconds(), TimeUnit.SECONDS);

        // keeper-tfs-m6 §11.2
        lastMetaFlushMilli = System.currentTimeMillis();
        long checkPeriod = metaFlushCheckPeriodMilli;
        metaFlushFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                checkAndFlushMeta();
            }
        }, checkPeriod, checkPeriod, TimeUnit.MILLISECONDS);
    }

    /** Takes effect on the next {@code start()}. */
    @VisibleForTesting
    public void setMetaFlushCheckPeriodMilli(long metaFlushCheckPeriodMilli) {
        this.metaFlushCheckPeriodMilli = metaFlushCheckPeriodMilli;
    }

    private void checkAndFlushMeta() {
        long interval = keeperConfig.getMetaFlushIntervalMilli();
        if (interval <= 0) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastMetaFlushMilli < interval) {
            return;
        }
        lastMetaFlushMilli = now;
        flushMetaPeriodically();
    }

    void flushMetaPeriodically() {
        ReplicationStore store = flushManagerMeta();
        if (store == null || !store.checkOk()) {
            return;
        }
        try {
            store.flushMeta();
        } catch (Throwable th) {
            logger.warn("[flushMetaPeriodically][store meta fail] repl_id={} store={} cause: {}", replId, store,
                    ExceptionUtils.getTypeAndMessage(th));
        }
    }

    /**
     * Rewrites manager meta from {@code currentStore} through the already-open handle only.
     *
     * @return {@code currentStore} read under the monitor, or null when the whole tick must be skipped
     */
    synchronized ReplicationStore flushManagerMeta() {
        if (readOnly || !getLifecycleState().isStarted() || managerMetaDestroyed) {
            return null;
        }
        ReplicationStore store = currentStore.get();
        if (store == null || !store.checkOk()) {
            return null;
        }
        AsyncFile asyncFile = managerMetaAsyncFile;
        if (asyncFile == null || currentStoreDirName == null) {
            return store;
        }
        String path = metaFile.getAbsolutePath();
        try {
            Properties meta = new Properties();
            Properties committed = currentMeta.get();
            if (committed != null) {
                meta.putAll(committed);
            }
            meta.setProperty(LATEST_STORE_DIR, currentStoreDirName);
            AsyncFileSystemHelper.writeAllBytes(asyncFileSystem, asyncFile, encodeMeta(meta),
                    "flush manager meta " + path);
            currentMeta.set(meta);
            logger.debug("[flushManagerMeta][ok] repl_id={} path={}", replId, path);
        } catch (Throwable th) {
            logger.warn("[flushManagerMeta][fail] repl_id={} path={} cause: {}", replId, path,
                    ExceptionUtils.getTypeAndMessage(th));
        }
        return store;
    }

    /**
     * PREPARE / Server stop: cancel GC → best-effort flush → close store handles (not destroy)
     * → close manager-meta long-lived handle (reopen lazily after start).
     * <p>
     * {@code releaseCurrentStore} failures are swallowed (align {@link #doDispose}): prefer reaching
     * Lifecycle STOPPED over propagating — {@link com.ctrip.xpipe.lifecycle.AbstractLifecycle#stop}
     * rollback would leave STARTED with GC/store/meta already torn down. Manager-meta close is in
     * {@code finally}. Failures are recorded for {@link #takeStopReleaseFailed()}.
     */
    @Override
    protected void doStop() throws Exception {
        cancelGcFuture();
        cancelMetaFlushFuture();
        flushStoreBestEffort();
        boolean storeReleased = false;
        try {
            storeReleased = releaseCurrentStore(false);
        } catch (Exception e) {
            logger.warn("[doStop][releaseCurrentStore]", e);
        } finally {
            boolean metaClosed = closeManagerMetaFile();
            stopReleaseFailed.set(!storeReleased || !metaClosed);
        }
    }

    @Override
    public boolean takeStopReleaseFailed() {
        return stopReleaseFailed.getAndSet(false);
    }

    @Override
    protected void doDispose() throws Exception {
        cancelGcFuture();
        cancelMetaFlushFuture();
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

    private void cancelMetaFlushFuture() {
        ScheduledFuture<?> future = metaFlushFuture;
        metaFlushFuture = null;
        if (future != null) {
            future.cancel(true);
        }
    }

    /**
     * Best-effort index flush before close. Failures are WARN-only
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
    public synchronized boolean releaseCurrentStore(boolean ignoreCloseFail) throws IOException {
        logger.info("[releaseCurrentStore][ignoreCloseFail={}]{}", ignoreCloseFail, this);
        ReplicationStore replicationStore = currentStore.get();
        if (replicationStore == null) {
            return true;
        }
        try {
            return replicationStore.close(ignoreCloseFail);
        } finally {
            // Always drop the lease reference so PREPARE cannot reopen via getCurrent.
            currentStore.set(null);
            currentStoreDirName = null;
        }
    }

    @Override
    public synchronized ReplicationStore createIfNotExist() throws IOException {

        if (readOnly) {
            return getCurrent();
        }

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

        checkNotReadOnly();
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

        // T-H2.E1 (方案 B): construct before publishing latest — store construction / createCommandStore
        // failure throws here before recordLatestStore / releaseCurrentStore / currentStore.set, so the old
        // store stays open, latest.store.dir keeps the old value, and getCurrent() still returns the old store.
        // The orphaned empty UUID dir is reclaimed by Manager gc (name != LATEST_STORE_DIR).
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
        currentStoreDirName = storeBaseDir.getName();

        notifyObservers(new NodeAdded<ReplicationStore>(replicationStore));
        return currentStore.get();
    }

    /** STOPPING window or after stop/dispose — refuse reopen / create / self-heal (T-S.12). */
    private boolean refuseOpenOrCreate() {
        return getLifecycleState().isStopping() || getLifecycleState().isPositivelyStopped();
    }

    /**
     * Bind the ACTIVE/BACKUP PUBLISH parse hook when a writable store is constructed.
     * Each store object is created once ({@code create()} / {@code getCurrent()} reopen), so uniqueness
     * follows Manager lifecycle — no Server-side identity cache. Read-only stores never append, skip.
     */
    public void setPubSubParseHook(KeeperPubSubParseHook pubSubParseHook) {
        this.pubSubParseHook = pubSubParseHook;
    }

    protected ReplicationStore createReplicationStore(File storeBaseDir, KeeperConfig keeperConfig, String keeperRunid,
                                                      KeeperMonitor keeperMonitor, SyncRateManager syncRateManager) throws IOException {
        ReplicationStore replicationStore = new GtidReplicationStore(this.ckStore, this.masterEventLoopGroup, storeBaseDir, keeperConfig, keeperRunid, keeperMonitor, redisOpParser,
                syncRateManager, asyncFileSystem, replId, this.readOnly);
        bindPubSubParseHook(replicationStore);
        return replicationStore;
    }

    private void bindPubSubParseHook(ReplicationStore store) {
        KeeperPubSubParseHook hook = this.pubSubParseHook;
        if (hook == null || readOnly || !(store instanceof DefaultReplicationStore)) {
            return;
        }
        hook.reset();
        ((DefaultReplicationStore) store).setPubSubParseHook(hook);
    }

    synchronized void recordLatestStore(String storeDir) throws IOException {
        checkNotReadOnly();
        Properties meta = new Properties();
        meta.putAll(currentMeta());
        meta.setProperty(LATEST_STORE_DIR, storeDir);
        saveMeta(meta);
    }

    private synchronized void saveMeta(Properties meta) throws IOException {
        byte[] data = encodeMeta(meta);

        AsyncFile asyncFile = getOrOpenManagerMetaFile();
        Properties committed = currentMeta.get();
        // keeper-tfs-m6 §11.5
        AsyncFileSystemHelper.writeAllBytes(asyncFileSystem, asyncFile, data,
                committed == null ? null : encodeMeta(committed), "write manager meta " + metaFile.getAbsolutePath());

        logger.info("[saveMeta][before]{}", currentMeta.get());
        currentMeta.set(meta);
        logger.info("[saveMeta][after]{}", currentMeta.get());
    }

    private static byte[] encodeMeta(Properties meta) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        meta.store(out, null);
        return out.toByteArray();
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
        if (readOnly) {
            AsyncFile asyncFile = AsyncFileSystemHelper.awaitOpen(asyncFileSystem,
                    () -> asyncFileSystem.open(metaFile.getAbsolutePath(), AbstractStorageFile.OpenMode.READ,
                            AbstractStorageFile.ReplaceMode.ATOMIC_PREFER_TMP, true,
                            replId.toString()),
                    "open manager meta " + metaFile.getAbsolutePath());
            managerMetaAsyncFile = asyncFile;
            return managerMetaAsyncFile;
        }
        // Parent dir may not exist before first create(); open(CREATE) needs it.
        AsyncFileSystemHelper.await(() -> asyncFileSystem.mkdir(baseDir.getAbsolutePath(), true),
                "mkdir manager baseDir for meta " + baseDir.getAbsolutePath());
        AsyncFile asyncFile = AsyncFileSystemHelper.awaitOpen(asyncFileSystem, () -> asyncFileSystem.open(metaFile.getAbsolutePath(), AbstractStorageFile.OpenMode.READ_WRITE,
                        AbstractStorageFile.ReplaceMode.ATOMIC, true,
                        replId.toString()),
                "open manager meta " + metaFile.getAbsolutePath());
        managerMetaAsyncFile = asyncFile;
        return managerMetaAsyncFile;
    }

    /**
     * Idempotent close of manager-meta handle. Decoupled from {@link #releaseCurrentStore()};
     * called from stop/dispose/destroy. Must not reopen until lifecycle leaves stopped
     * ({@link #getOrOpenManagerMetaFile()} gated).
     *
     * @return false only when a write handle was left for asynchronous close
     */
    private synchronized boolean closeManagerMetaFile() {
        if (managerMetaAsyncFile == null) {
            return true;
        }
        AsyncFile toClose = managerMetaAsyncFile;
        managerMetaAsyncFile = null;
        boolean closed = AsyncFileSystemHelper.closeHandle(asyncFileSystem, toClose, replId,
                "close manager meta " + metaFile.getAbsolutePath());
        return closed || readOnly;
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
                        currentStoreDirName = latestStoreDir.getName();
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

        checkNotReadOnly();
        logger.debug("[gc]{}", this);

        if (!getLifecycleState().isStarted()) {
            logger.info("[gc][not started][skip]{}", this);
            return;
        }

        gcCount.incrementAndGet();
        // keeper-tfs-m6 §11.5: no force load
        Properties meta = currentMeta();
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

        // gc the opened store only: GC must never reopen latest.store.dir (m6 spec §9.3)
        ReplicationStore replicationStore = currentStore.get();
        if (replicationStore != null && replicationStore.checkOk()) {
            replicationStore.gc();
        }
    }

    @Override
    public synchronized void destroy() throws Exception {
        logger.info("[destroy]{}", this);
        // Permanent refuse reopen before close/rmdir so create/getCurrent/gc cannot race a new open.
        managerMetaDestroyed = true;
        try {
            // Drain in-flight atomic replace (TMP_REP_*) before walkFileTree; close awaits FS in-flight.
            releaseCurrentStore();
        } catch (Exception e) {
            logger.warn("[destroy][releaseCurrentStore]", e);
        } finally {
            closeManagerMetaFile();
        }
        AsyncFileSystemHelper.await(() -> asyncFileSystem.rmdir(this.baseDir.getAbsolutePath(), true),
                "rmdir replication store manager baseDir " + baseDir);
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        if (getLifecycleState().isStarted()) {
            throw new IllegalStateException("setReadOnly only allowed when manager is not started: " + this);
        }
        if (currentStore.get() != null) {
            throw new IllegalStateException("setReadOnly requires currentStore == null: " + this);
        }
        this.readOnly = readOnly;
        currentMeta.set(null);
        logger.info("[setReadOnly]{} {}", readOnly, this);
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public ReplicationStore getOpenedStore() {
        return currentStore.get();
    }

    /**
     * Watcher-only 关相入口 (D48). Read-only mode: close the manager-meta handle and <b>keep the
     * {@code currentMeta} cache</b> — a request-side {@code getCurrent()} during the closed phase must
     * read memory instead of triggering a lazy reopen that would ruin the quiet window (§4.2.3b).
     */
    @Override
    public synchronized void closeReadOnlyMetaHandle() {
        if (!readOnly) {
            return;
        }
        closeManagerMetaFile();
    }

    /**
     * Whether the {@code store_manager_meta.properties} handle is currently open (D48 phase fact).
     */
    @VisibleForTesting
    public synchronized boolean isManagerMetaHandleOpen() {
        return managerMetaAsyncFile != null;
    }

    /**
     * Cached {@code store_manager_meta.properties} only. Does not load or open a handle.
     */
    @VisibleForTesting
    public Properties getCurrentMetaCache() {
        return currentMeta.get();
    }

    @Override
    public synchronized String reloadLatestStoreDir() throws IOException {
        // No close here (D48): the handle is closed by the watcher's closed phase and stays quiet for
        // >= keeper.prepare.watch.close.hold.milli before this force-load reopens it lazily.
        currentMeta.set(null);
        Properties meta = currentMeta(true);
        if (meta == null) {
            return null;
        }
        return meta.getProperty(LATEST_STORE_DIR);
    }

    private void checkNotReadOnly() {
        if (readOnly) {
            throw new IllegalStateException(READ_ONLY_STORE_MSG);
        }
    }

    public long getGcCount() {
        return gcCount.get();
    }

    @Override
    public String toString() {
        return String.format("repl:%s, keeperRunId:%s, baseDir:%s, currentMeta:%s", replId, keeperRunid, baseDir,
                currentMeta.get() == null ? "" : currentMeta.get().toString());
    }

    @Override
    public File getBaseDir() {
        return baseDir;
    }

}
