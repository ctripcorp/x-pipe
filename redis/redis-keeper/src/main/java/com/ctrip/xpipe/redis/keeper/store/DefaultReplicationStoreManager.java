package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.redis.core.util.NonFinalizeFileInputStream;
import com.ctrip.xpipe.redis.core.util.NonFinalizeFileOutputStream;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.util.KeeperReplIdAwareThreadFactory;
import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;
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

    public DefaultReplicationStoreManager(KeeperConfig keeperConfig, ReplId replId,
                                          String keeperRunid, File baseDir, KeeperMonitor keeperMonitor, SyncRateManager syncRateManager) {
        this(keeperConfig, replId, keeperRunid, baseDir, keeperMonitor, syncRateManager, null);
    }

    public DefaultReplicationStoreManager(KeeperConfig keeperConfig, ReplId replId,
                                          String keeperRunid, File baseDir, KeeperMonitor keeperMonitor,
                                          SyncRateManager syncRateManager, RedisOpParser redisOpParser) {
        super(MoreExecutors.directExecutor());
        this.replId = replId;
        this.keeperRunid = keeperRunid;
        this.keeperConfig = keeperConfig;
        this.keeperMonitor = keeperMonitor;
        this.keeperBaseDir = baseDir;
        this.redisOpParser = redisOpParser;
        this.syncRateManager = syncRateManager;
    }

    public DefaultReplicationStoreManager(CKStore ckStore, KeeperConfig keeperConfig, ReplId replId,
                                          String keeperRunid, File baseDir, KeeperMonitor keeperMonitor,
                                          SyncRateManager syncRateManager, RedisOpParser redisOpParser) {
        this(keeperConfig, replId, keeperRunid, baseDir, keeperMonitor, syncRateManager, redisOpParser);
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
        storeBaseDir.mkdirs();

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
        return new GtidReplicationStore(this.ckStore,storeBaseDir,keeperConfig,keeperRunid, keeperMonitor, redisOpParser, syncRateManager);
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
        try (OutputStream out = new NonFinalizeFileOutputStream(metaFile)) {
            meta.store(out, null);
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

        if (metaFile.isFile()) {
            Properties meta = new Properties();
            try (InputStream in = new NonFinalizeFileInputStream(metaFile)) {
                meta.load(in);
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
                    if (latestStoreDir.isDirectory()) {
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
        final String currentDirName;
        if (meta != null) {
            currentDirName = meta.getProperty(LATEST_STORE_DIR);
            File[] replicationStoreDirs = baseDir.listFiles(new FileFilter() {

                @Override
                public boolean accept(File path) {
                    return path.isDirectory() && !currentDirName.equals(path.getName());
                }
            });

            if (replicationStoreDirs != null && replicationStoreDirs.length > 0) {

                logger.info("[GC][old replicationstore]newest:{}", currentDirName);
                for (File dir : replicationStoreDirs) {
                    if (System.currentTimeMillis() - dir.lastModified() > keeperConfig.getReplicationStoreMinTimeMilliToGcAfterCreate()) {
                        logger.info("[GC] directory {}", dir.getCanonicalPath());
                        FileUtils.recursiveDelete(dir);
                    } else {
                        logger.warn("[GC][directory is created too short, do not gc]{}, {}", dir, new Date(dir.lastModified()));
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
        FileUtils.recursiveDelete(this.baseDir);
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

    public static class ClusterAndShardCompatible extends DefaultReplicationStoreManager {

        private final Logger logger = LoggerFactory.getLogger(ClusterAndShardCompatible.class);

        private final File keeperBaseDir;

        private ReplId replId;

        private ClusterId deprecatedClusterId;

        private ShardId deprecatedShardId;

        public ClusterAndShardCompatible(CKStore ckStore,KeeperConfig keeperConfig, ReplId replId, String keeperRunid,
                                         File baseDir, KeeperMonitor keeperMonitor, RedisOpParser redisOpParser,
                                         SyncRateManager syncRateManager) {
            super(ckStore,keeperConfig, replId, keeperRunid, baseDir, keeperMonitor, syncRateManager, redisOpParser);
            this.keeperBaseDir = baseDir;
            this.replId = replId;
        }

        @Override
        protected void doInitialize() throws Exception {

            renameDeprecatedStore();

            super.doInitialize();
        }

        public ClusterAndShardCompatible setDeprecatedClusterAndShard(ClusterId clusterId, ShardId shardId) {
            this.deprecatedClusterId = clusterId;
            this.deprecatedShardId = shardId;
            return this;
        }

        public void renameDeprecatedStore() {
            if (null == deprecatedClusterId || null == deprecatedShardId) {
                return;
            }

            File deprecated = new File(keeperBaseDir, deprecatedClusterId + "/" + deprecatedShardId);
            File dest = new File(keeperBaseDir, replId.toString());

            File deprecatedParent = new File(keeperBaseDir, deprecatedClusterId.toString());
            if (deprecated.exists() && !dest.exists()) {
                try {
                    keeperBaseDir.mkdirs();
                    Files.move(deprecated, dest);
                    logger.info("[renameDeprecatedStore] {} -> {} success", deprecated.getAbsolutePath(), dest.getAbsolutePath());
                    FileUtils.recursiveDelete(deprecatedParent);
                } catch (IOException e) {
                    logger.error("[renameDeprecatedStore] {} -> {} failure", deprecated.getAbsolutePath(), dest.getAbsolutePath(), e);
                }
            }
        }
    }

}
