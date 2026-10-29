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

            return deleteMetaThenDir(baseDir, replId, metaPath, meta);
        } finally {
            AsyncFileSystemHelper.closeHandle(fs, meta, replId, "delete store close meta " + metaPath);
        }
    }

    /**
     * destroy with the manager's still-open meta write handle; never opens meta. With a handle: rewrite the
     * current content through it to prove this process still owns the store, then delete meta and the
     * directory. Without one: an existing meta may belong to a live writer, so leave it to the orphan GC.
     */
    @Override
    public GcOrphanStoreResult delete(File baseDir, ReplId replId, AsyncFile heldMeta) {
        String metaPath = new File(baseDir, META_FILE).getAbsolutePath();
        if (heldMeta == null) {
            try {
                if (AsyncFileSystemHelper.await(() -> fs.exists(metaPath), "destroy meta exists " + metaPath)) {
                    return GcOrphanStoreResult.skip(replId.id(), "no held meta handle, left to orphan gc");
                }
            } catch (Throwable th) {
                logger.warn("[delete][meta exists check failed, skip]{}", baseDir, th);
                return GcOrphanStoreResult.skip(replId.id(), "meta exists check failed: " + th.getMessage());
            }
            return deleteWithoutMeta(baseDir, replId);
        }
        try {
            try {
                long size = AsyncFileSystemHelper.await(() -> fs.size(heldMeta), "destroy stat meta " + metaPath);
                byte[] data = size == 0 ? new byte[0]
                        : AsyncFileSystemHelper.readAllBytes(fs, heldMeta, size, 0, "destroy read meta " + metaPath);
                AsyncFileSystemHelper.writeAllBytes(fs, heldMeta, data, "destroy rewrite meta " + metaPath);
            } catch (Throwable th) {
                logger.warn("[delete][held meta not writable, skip]{}", baseDir, th);
                return GcOrphanStoreResult.skip(replId.id(), "held meta not writable: " + th.getMessage());
            }
            return deleteMetaThenDir(baseDir, replId, metaPath, heldMeta);
        } finally {
            AsyncFileSystemHelper.closeHandle(fs, heldMeta, replId, "destroy close meta " + metaPath);
        }
    }

    /** Deletes meta first, then the directory; meta gone means the store is dead even if rmdir fails. */
    private GcOrphanStoreResult deleteMetaThenDir(File baseDir, ReplId replId, String metaPath, AsyncFile meta) {
        try {
            AsyncFileSystemHelper.await(() -> fs.delete(meta), "delete store meta " + metaPath);
        } catch (Throwable th) {
            logger.error("[delete][delete meta failed, data untouched]{}", baseDir, th);
            return GcOrphanStoreResult.fail(replId.id(), "delete meta failed: " + th.getMessage());
        }
        return deleteWithoutMeta(baseDir,replId);
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
