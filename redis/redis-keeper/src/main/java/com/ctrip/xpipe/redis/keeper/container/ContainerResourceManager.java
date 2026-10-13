package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncTFSBasedFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleCloser;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystemConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author lishanglin
 * date 2022/6/9
 */
@Component
public class ContainerResourceManager {

    private static final Logger logger = LoggerFactory.getLogger(ContainerResourceManager.class);

    private Set<Integer> runningPorts = Sets.newConcurrentHashSet();

    @Autowired
    private KeeperConfig keeperConfig;

    private volatile AsyncFileSystem asyncFileSystem;

    // keeper-tfs-m6 §2.4: container level, outlives every keeper; both guarded by this.
    private DeferredHandleCloser deferredHandleCloser;
    private UnclosedHandleMetricReporter unclosedHandleMetricReporter;

    public boolean isPortFree(int port) {
        return runningPorts.contains(port);
    }

    public boolean applyPort(int port) {
        return runningPorts.add(port);
    }

    public boolean releasePort(int port) {
        return runningPorts.remove(port);
    }

    public AsyncFileSystem getAsyncFileSystem() {
        AsyncFileSystem fs = asyncFileSystem;
        if (fs != null) {
            return fs;
        }
        synchronized (this) {
            if (asyncFileSystem == null) {
                AsyncFileSystem created = createAsyncFileSystem(keeperConfig);
                DeferredHandleCloser closer = new DeferredHandleCloser(created,
                        keeperConfig::getUnclosedHandleFlushRetryWindowMilli);
                // Registered before the FS is published, so no close on it can miss the closer.
                DeferredHandleCloser.register(created, closer);
                deferredHandleCloser = closer;
                unclosedHandleMetricReporter = new UnclosedHandleMetricReporter(closer);
                unclosedHandleMetricReporter.start();
                asyncFileSystem = created;
            }
            return asyncFileSystem;
        }
    }

    /** The container FS's deferred closer; null until the FS is created or after it is shut down. */
    public synchronized DeferredHandleCloser getDeferredHandleCloser() {
        return deferredHandleCloser;
    }

    /**
     * Stops the deferred closer before the FS (spec §2.4): retries must not run against an FS that
     * is shutting down; handles still pending are logged by the closer and left to the FS shutdown.
     */
    public synchronized void shutdownAsyncFileSystem() {
        if (asyncFileSystem != null) {
            try {
                if (unclosedHandleMetricReporter != null) {
                    unclosedHandleMetricReporter.stop();
                    unclosedHandleMetricReporter = null;
                }
                if (deferredHandleCloser != null) {
                    deferredHandleCloser.shutdown();
                }
            } catch (Throwable t) {
                logger.error("[shutdownAsyncFileSystem] deferred closer shutdown failed", t);
            } finally {
                deferredHandleCloser = null;
                asyncFileSystem.shutdown();
                asyncFileSystem = null;
            }
        }
    }

    public static AsyncFileSystem createAsyncFileSystem(KeeperConfig keeperConfig) {
        TailCacheFileSystemConfig cacheConfig = new TailCacheFileSystemConfig()
                .setMaxCacheSizeBytes(keeperConfig.getAsyncTailCacheMaxSizeBytes());
        return createAsyncFileSystem(keeperConfig.getAsyncIoThreads(), keeperConfig.getAsyncFsyncIntervalBytes(),
                keeperConfig.getAsyncFsyncIntervalMillis(), cacheConfig);
    }

    public static AsyncFileSystem createAsyncFileSystem(int ioThreads, long fsyncIntervalBytes) {
        return createAsyncFileSystem(ioThreads, fsyncIntervalBytes, KeeperConfig.DEFAULT_ASYNC_FSYNC_INTERVAL_MILLIS);
    }

    public static AsyncFileSystem createAsyncFileSystem(int ioThreads, long fsyncIntervalBytes,
            long fsyncIntervalMillis) {
        return createAsyncFileSystem(ioThreads, fsyncIntervalBytes, fsyncIntervalMillis,
                new TailCacheFileSystemConfig());
    }

    public static AsyncFileSystem createAsyncFileSystem(int ioThreads, long fsyncIntervalBytes,
            long fsyncIntervalMillis, TailCacheFileSystemConfig cacheConfig) {
        if (cacheConfig == null) {
            throw new IllegalArgumentException("cacheConfig must not be null");
        }
        logger.info("[createAsyncFileSystem]ioThreads={}, fsyncIntervalBytes={}, fsyncIntervalMillis={}, maxCacheSizeBytes={}",
                ioThreads, fsyncIntervalBytes, fsyncIntervalMillis, cacheConfig.getMaxCacheSizeBytes());
        ExecutorService ioExecutor = Executors.newFixedThreadPool(ioThreads,
                XpipeThreadFactory.create("keeper-async-io"));
        AsyncTFSBasedFileSystem backing = new AsyncTFSBasedFileSystem(ioExecutor, fsyncIntervalBytes,
                fsyncIntervalMillis);
        return new TailCacheFileSystem(backing, cacheConfig, ioExecutor);
    }
}
