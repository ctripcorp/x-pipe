package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * TFS: the store directory is shared, so it is removed only when nobody can be using it. Holds a write
 * handle on {@code store_manager_meta.properties} from the checks until both deletes finish, and deletes
 * the meta file before the directory so a crash never leaves a store that still looks live.
 */
public final class TfsStoreDeleter implements ReplicationStoreDeleter {

    private static final Logger logger = LoggerFactory.getLogger(TfsStoreDeleter.class);

    private final AsyncFileSystem fs;

    /** 0 skips the idle check (destroy); the orphan GC passes its idle threshold. */
    private final long minIdleMilli;

    public TfsStoreDeleter(AsyncFileSystem fs, long minIdleMilli) {
        this.fs = fs;
        this.minIdleMilli = minIdleMilli;
    }

    @Override
    public GcOrphanStoreResult delete(File baseDir, ReplId replId) {
        String metaPath = new File(baseDir, META_FILE).getAbsolutePath();
        AsyncFile meta = null;
        try {
            try {

                long newest = getCurrentDirLastModified(baseDir);
                if (tooFresh(newest)) {
                    return GcOrphanStoreResult.skip(replId.id(), "store dir without meta modified too recently");
                }

                if (!AsyncFileSystemHelper.await(() -> fs.exists(metaPath), "delete store meta exists " + metaPath)) {
                    return deleteWithoutMeta(baseDir, replId);
                }
                // Fails if another writer holds the meta file; once open, blocks writers until finally.
                meta = AsyncFileSystemHelper.awaitOpen(fs, () -> fs.open(metaPath, AbstractStorageFile.OpenMode.READ_WRITE,
                        AbstractStorageFile.ReplaceMode.ATOMIC, true, replId.toString()), "delete store open meta " + metaPath);
            } catch (Throwable th) {
                logger.warn("[delete][meta not usable, skip]{}", baseDir, th);
                return GcOrphanStoreResult.skip(replId.id(), "meta not usable: " + th.getMessage());
            }

            AsyncFile held = meta;
            try {
                AsyncFileSystemHelper.await(() -> fs.delete(held), "delete store meta " + metaPath);
            } catch (Throwable th) {
                logger.error("[delete][delete meta failed, data untouched]{}", baseDir, th);
                return GcOrphanStoreResult.fail(replId.id(), "delete meta failed: " + th.getMessage());
            }
            // Meta is gone, so the store is already dead; a leftover directory is retried via deleteWithoutMeta.
            rmdir(baseDir);
            return GcOrphanStoreResult.deleted(replId.id());
        } finally {
            AsyncFileSystemHelper.closeHandle(fs, meta, replId, "delete store close meta " + metaPath);
        }
    }

    /**
     * No meta file: the directory may be a store still being created (mkdir comes before the first meta
     * write), so it is removed only when the directory and its direct children are all idle. Uses
     * java.io directly: AsyncFileSystem has no path-level mtime, and its list() hides TMP_REP_ files.
     */
    private GcOrphanStoreResult deleteWithoutMeta(File baseDir, ReplId replId) {
        if (!baseDir.exists()) {
            return GcOrphanStoreResult.notFound(replId.id());
        }
        return rmdir(baseDir) ? GcOrphanStoreResult.deleted(replId.id())
                : GcOrphanStoreResult.fail(replId.id(), "rmdir store dir without meta failed");
    }


    private long getCurrentDirLastModified(File dir){
        long newest = dir.lastModified();
        File[] children = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                newest = Math.max(newest, child.lastModified());
            }
        }
        return newest;
    }

    private boolean tooFresh(long mtime) {
        return minIdleMilli > 0 && System.currentTimeMillis() - mtime < minIdleMilli;
    }

    private boolean rmdir(File baseDir) {
        try {
            AsyncFileSystemHelper.await(() -> fs.rmdir(baseDir.getAbsolutePath(), true), "delete store rmdir " + baseDir);
            return true;
        } catch (Throwable th) {
            logger.warn("[delete][rmdir failed]{}", baseDir, th);
            return false;
        }
    }
}
