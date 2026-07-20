package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncTFSBasedFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileSystemConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Sets;
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

    private Set<Integer> runningPorts = Sets.newConcurrentHashSet();

    @Autowired
    private KeeperConfig keeperConfig;

    private volatile AsyncFileSystem asyncFileSystem;

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
                asyncFileSystem = createAsyncFileSystem(keeperConfig);
            }
            return asyncFileSystem;
        }
    }

    public synchronized void shutdownAsyncFileSystem() {
        if (asyncFileSystem != null) {
            asyncFileSystem.shutdown();
            asyncFileSystem = null;
        }
    }

    public static AsyncFileSystem createAsyncFileSystem(KeeperConfig keeperConfig) {
        return createAsyncFileSystem(keeperConfig.getAsyncIoThreads(), keeperConfig.getAsyncFsyncIntervalBytes(),
                keeperConfig.getAsyncFsyncIntervalMillis());
    }

    public static AsyncFileSystem createAsyncFileSystem(int ioThreads, long fsyncIntervalBytes) {
        return createAsyncFileSystem(ioThreads, fsyncIntervalBytes, KeeperConfig.DEFAULT_ASYNC_FSYNC_INTERVAL_MILLIS);
    }

    public static AsyncFileSystem createAsyncFileSystem(int ioThreads, long fsyncIntervalBytes,
            long fsyncIntervalMillis) {
        ExecutorService ioExecutor = Executors.newFixedThreadPool(ioThreads,
                XpipeThreadFactory.create("keeper-async-io"));
        AsyncTFSBasedFileSystem backing = new AsyncTFSBasedFileSystem(ioExecutor, fsyncIntervalBytes,
                fsyncIntervalMillis);
        return new TailCacheFileSystem(backing, new TailCacheFileSystemConfig(), ioExecutor);
    }
}
