package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
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

    /** TFS: the store dir is shared by every keeper of the shard; only the slot holder may mutate it. */
    private volatile boolean sharedStore;

    /** Slot holder flag, granted by the keeper state machine (ACTIVE / BACKUP). Ignored for a private store. */
    private volatile boolean storeWriteOwner;

    static final String NOT_STORE_WRITE_OWNER_MSG = "not store write owner";

    private volatile KeeperPubSubParseHook pubSubParseHook;

    private final KeeperConfig keeperConfig;

    private final AtomicLong gcCount = new AtomicLong();

    private ScheduledFuture<?> gcFuture;

    private ScheduledExecutorService scheduled;

    private final KeeperMonitor keeperMonitor;

    private final RedisOpParser redisOpParser;

    private SyncRateManager syncRateManager;

    private CKStore ckStore;

    private NioEventLoopGroup masterEventLoopGroup;

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

    public DefaultReplicationStoreManager(CKStore ckStore, NioEventLoopGroup masterEventLoopGroup, KeeperConfig keeperConfig, ReplId replId,
                                          String keeperRunid, File baseDir, KeeperMonitor keeperMonitor,
                                          SyncRateManager syncRateManager, RedisOpParser redisOpParser,
                                          ScheduledExecutorService commandNotifyScheduler,
                                          AsyncFileSystem asyncFileSystem) {
        this(keeperConfig, replId, keeperRunid, baseDir, keeperMonitor, syncRateManager, redisOpParser, commandNotifyScheduler, asyncFileSystem);
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
     * Start Manager GC. PREPARE → ACTIVE/BACKUP (Phase Rc) re-enters via {@code start()} again.
     * Read-only mode skips GC (D7b).
     */
    @Override
    protected void doStart() throws Exception {
        if (readOnly) {
            logger.info("[doStart][readOnly][skip gc]{}", this);
            return;
        }
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

        if (readOnly) {
            return getCurrent();
        }

        // Shared store, not the slot holder: never open RW or create(). Fail loudly instead of
        // returning null so callers (INFO / PSYNC / searcher ...) cannot mistake it for "fresh".
        checkStoreWriteOwner();

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
        checkStoreWriteOwner();
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
                syncRateManager, commandNotifyScheduler, asyncFileSystem, replId, this.readOnly);
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

    void recordLatestStore(String storeDir) throws IOException {
        checkNotReadOnly();
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
        return parseMeta(getOrOpenManagerMetaFile());
    }

    /**
     * 从<b>已打开</b>的句柄读 size → read → parse。不负责 open / close，也不碰 {@code currentMeta} 缓存。
     */
    private Properties parseMeta(AsyncFile asyncFile) throws IOException {
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

    /**
     * <b>⚠️ 这个方法可能取得写租约。</b>{@code currentStore == null} 时它不只构造 store 对象，
     * 还会：{@code mkdir(baseDir)}、以 {@code READ_WRITE} 打开共享的 store_manager_meta、
     * 再写打开 store 目录 —— 对 TFS 模式即"取得共享目录的写租约"。
     * <p>
     * 共享 store（TFS）由写权闸门约束：非占槽者（未被授予 {@link #setStoreWriteOwner}）在非只读模式下
     * 这里返回 {@code null}、不开店；只读模式照常只读打开。只读命令、观察者等不需要开店的路径
     * 仍应使用 {@link #getOpenedStore()}，它从不触发 FS。
     */
    @Override
    public synchronized ReplicationStore getCurrent() throws IOException {

        if (currentStore.get() == null) {
            if (refuseOpenOrCreate()) {
                logger.info("[getCurrent][stopping/stopped][skip reopen]{}", this);
                return null;
            }
            // Shared store: a non-holder must not open RW (lease + recoverIndex + rdb cleanup).
            // Read-only mode is fine: it only reads.
            if (!readOnly && !mayWriteStore()) {
                logger.debug("[getCurrent][not store write owner][skip open]{}", this);
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

        checkNotReadOnly();
        logger.debug("[gc]{}", this);

        if (!getLifecycleState().isStarted()) {
            logger.info("[gc][not started][skip]{}", this);
            return;
        }

        gcCount.incrementAndGet();

        // 没有已打开的 store 时【整个返回】，连上半段的目录清理也不做。
        //
        // 为什么可以跳过目录清理：非 latest 目录只可能由调过 create() 的 keeper 产生
        // （换代遗留 / create() 中途失败留下的空目录），而它必然 currentStore != null，
        // 所以它不会跳过 —— 垃圾仍有占槽者清理。反过来，没有 store 的 keeper
        // （PREPARE / 未分配角色）本来就不该在【共享】baseDir 上执行删除。
        //
        // 这一条同时关掉两个与角色无关的获取入口：
        //   - :595 的 currentMeta(true) → loadMeta() → getOrOpenManagerMetaFile()
        //         会 mkdir(baseDir) + 以 READ_WRITE 打开共享的 store_manager_meta
        //   - 末尾的 getCurrent() 会构造并写打开 store 目录
        // 两者都是"每 2 秒一次"的抢占机会。
        if (currentStore.get() == null) {
            logger.debug("[gc][no store opened, skip]{}", this);
            return;
        }
        // Ownership revoked while a store is still open (PREPARE in progress): no deletes.
        if (!mayWriteStore()) {
            logger.debug("[gc][not store write owner, skip]{}", this);
            return;
        }

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
        try {
            // Drain in-flight atomic replace (TMP_REP_*) before walkFileTree; close awaits FS in-flight.
            releaseCurrentStore();
        } catch (Exception e) {
            logger.warn("[destroy][releaseCurrentStore]", e);
        } finally {
            closeManagerMetaFile();
        }
        // Shared store: removing one keeper must not delete the shard's data that the other
        // keepers (and the slot holder) still use. Whole-shard cleanup is not this keeper's call.
        if (sharedStore) {
            logger.info("[destroy][shared store, keep baseDir]{}", baseDir);
            return;
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

    /**
     * Whether this manager may touch the shared dir in write mode (open RW / create / gc / destroy).
     * Read-only mode never writes, so it does not need ownership.
     */
    private boolean mayWriteStore() {
        return !sharedStore || storeWriteOwner;
    }

    private void checkStoreWriteOwner() {
        if (!mayWriteStore()) {
            throw new IllegalStateException(NOT_STORE_WRITE_OWNER_MSG + ": " + this);
        }
    }

    @Override
    public void setSharedStore(boolean sharedStore) {
        this.sharedStore = sharedStore;
        logger.info("[setSharedStore]{} {}", sharedStore, this);
    }

    @Override
    public void setStoreWriteOwner(boolean owner) {
        if (this.storeWriteOwner != owner) {
            logger.info("[setStoreWriteOwner]{} {}", owner, this);
        }
        this.storeWriteOwner = owner;
    }

    @Override
    public boolean isStoreWriteOwner() {
        return mayWriteStore();
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
