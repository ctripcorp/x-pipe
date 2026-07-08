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

        gcFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                gc();
            }
        }, keeperConfig.getReplicationStoreGcIntervalSeconds(), keeperConfig.getReplicationStoreGcIntervalSeconds(), TimeUnit.SECONDS);
    }

    @Override
    protected void doDispose() throws Exception {
        closeCurrentStore();
        gcFuture.cancel(true);
        scheduled.shutdownNow();
    }

    private void closeCurrentStore() {

        logger.info("[closeCurrentStore]{}", this);
        ReplicationStore replicationStore = currentStore.get();
        if (replicationStore != null) {
            try {
                replicationStore.close();
                currentStore.set(null);
            } catch (IOException e) {
                logger.info("[close]" + replicationStore, e);
            }
        }
    }

    @Override
    public synchronized ReplicationStore createIfNotExist() throws IOException {

        ReplicationStore currentReplicationStore = null;

        try {
            currentReplicationStore = getCurrent();
        } catch (Exception e) {
            logger.error("[createIfNotExist]" + baseDir, e);
        }

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

        keeperMonitor.getReplicationStoreStats().increateReplicationStoreCreateCount();

        File storeBaseDir = new File(baseDir, UUID.randomUUID().toString());
        AsyncFileSystemHelper.await(asyncFileSystem.mkdir(storeBaseDir.getAbsolutePath(), true),
                "mkdir replication store " + storeBaseDir.getAbsolutePath());

        logger.info("[create]{}", storeBaseDir);

        recrodLatestStore(storeBaseDir.getName());

        ReplicationStore replicationStore = createReplicationStore(storeBaseDir, keeperConfig, keeperRunid, keeperMonitor, syncRateManager);

        closeCurrentStore();

        currentStore.set(replicationStore);

        notifyObservers(new NodeAdded<ReplicationStore>(replicationStore));
        return currentStore.get();
    }

    protected ReplicationStore createReplicationStore(File storeBaseDir, KeeperConfig keeperConfig, String keeperRunid,
                                                      KeeperMonitor keeperMonitor, SyncRateManager syncRateManager) throws IOException {
        return new GtidReplicationStore(this.ckStore,storeBaseDir,keeperConfig,keeperRunid, keeperMonitor, redisOpParser,
                syncRateManager, commandNotifyScheduler, asyncFileSystem, replId);
    }

    private void recrodLatestStore(String storeDir) throws IOException {
        Properties meta = currentMeta();
        if (meta == null) {
            meta = new Properties();
        }

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

        AsyncFile asyncFile = AsyncFileSystemHelper.await(
                asyncFileSystem.open(metaFile.getAbsolutePath(), true, true, true, replId.toString()),
                "open manager meta for write " + metaFile.getAbsolutePath());
        try {
            AsyncFileSystemHelper.await(asyncFileSystem.write(asyncFile, data, data.length),
                    "write manager meta " + metaFile.getAbsolutePath());
        } finally {
            AsyncFileSystemHelper.await(asyncFileSystem.close(asyncFile),
                    "close manager meta " + metaFile.getAbsolutePath());
        }

        logger.info("[saveMeta][before]{}", currentMeta.get());
        currentMeta.set(meta);
        logger.info("[saveMeta][after]{}", currentMeta.get());
    }

    /**
     * @return
     * @throws IOException
     */
    private Properties loadMeta() throws IOException {

        if (AsyncFileSystemHelper.await(asyncFileSystem.exists(metaFile.getAbsolutePath()),
                "check manager meta exists " + metaFile.getAbsolutePath())) {
            Properties meta = new Properties();
            AsyncFile asyncFile = AsyncFileSystemHelper.await(
                    asyncFileSystem.open(metaFile.getAbsolutePath(), false, false, true, replId.toString()),
                    "open manager meta for read " + metaFile.getAbsolutePath());
            try {
                long size = AsyncFileSystemHelper.await(asyncFileSystem.size(asyncFile),
                        "stat manager meta " + metaFile.getAbsolutePath());
                if (size > Integer.MAX_VALUE) {
                    throw new IOException("async file too large: " + metaFile.getAbsolutePath());
                }
                byte[] data = new byte[(int) size];
                long read = AsyncFileSystemHelper.await(asyncFileSystem.read(asyncFile, size, 0, data),
                        "read manager meta " + metaFile.getAbsolutePath());
                if (read != size) {
                    throw new IOException("failed to read full async file: " + metaFile.getAbsolutePath());
                }
                try (InputStream in = new ByteArrayInputStream(data)) {
                    meta.load(in);
                }
            } finally {
                AsyncFileSystemHelper.await(asyncFileSystem.close(asyncFile),
                        "close manager meta " + metaFile.getAbsolutePath());
            }
            return meta;
        }

        return null;
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
            Properties meta = currentMeta();
            if (meta != null) {
                if (meta.getProperty(LATEST_STORE_DIR) != null) {
                    File latestStoreDir = new File(baseDir, meta.getProperty(LATEST_STORE_DIR));
                    logger.info("[getCurrent][latest]{}", latestStoreDir);
                    if (AsyncFileSystemHelper.await(asyncFileSystem.exists(latestStoreDir.getAbsolutePath()),
                            "check latest store dir exists " + latestStoreDir.getAbsolutePath())) {
                        currentStore.set(createReplicationStore(latestStoreDir, keeperConfig, keeperRunid, keeperMonitor, syncRateManager));
                    }
                }
            }
        }

        ReplicationStore replicationStore = currentStore.get();
        if (replicationStore != null && !replicationStore.checkOk()) {
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

        gcCount.incrementAndGet();
        Properties meta = currentMeta(true);
        if (meta != null) {
            final String currentDirName = meta.getProperty(LATEST_STORE_DIR);
            List<String> children = AsyncFileSystemHelper.await(
                    asyncFileSystem.list(baseDir.getAbsolutePath()),
                    "list replication store manager baseDir " + baseDir);

            if (children != null && !children.isEmpty()) {

                logger.info("[GC][old replicationstore]newest:{}", currentDirName);
                for (String name : children) {
                    if (currentDirName != null && currentDirName.equals(name)) {
                        continue;
                    }
                    String childPath = new File(baseDir, name).getAbsolutePath();
                    boolean isDir = AsyncFileSystemHelper.await(
                            asyncFileSystem.isDirectory(childPath),
                            "isDirectory " + childPath);
                    if (!isDir) {
                        continue;
                    }
                    // TODO T-FS.14: replace with asyncFileSystem.lastModified(childPath) once path-level mtime lands.
                    long lastModified = new File(childPath).lastModified();
                    if (System.currentTimeMillis() - lastModified > keeperConfig.getReplicationStoreMinTimeMilliToGcAfterCreate()) {
                        logger.info("[GC] directory {}", childPath);
                        AsyncFileSystemHelper.await(
                                asyncFileSystem.rmdir(childPath, true),
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
    public void destroy() throws Exception {
        logger.info("[destroy]{}", this);
        AsyncFileSystemHelper.await(
                asyncFileSystem.rmdir(this.baseDir.getAbsolutePath(), true),
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
