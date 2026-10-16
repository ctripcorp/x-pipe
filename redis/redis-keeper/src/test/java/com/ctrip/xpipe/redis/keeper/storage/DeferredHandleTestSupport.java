package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.store.ReplId;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.mock;

/** Builds pending deferred handles for tests outside the storage package (file constructors are package-private). */
public final class DeferredHandleTestSupport {

    private DeferredHandleTestSupport() {
    }

    /** A closer whose retries are never run, so deferred handles stay pending. */
    public static DeferredHandleCloser idleCloser(AsyncFileSystem fs, LongSupplier clock, long flushWindowMillis) {
        return new DeferredHandleCloser(fs, () -> flushWindowMillis, clock, mock(ScheduledExecutorService.class));
    }

    public static void deferFile(DeferredHandleCloser closer, String path, boolean write, ReplId replId,
                                 String operation, Throwable cause) {
        String key = StorageUtil.asyncFileKey(path);
        AbstractStorageFile.OpenMode mode = write ? AbstractStorageFile.OpenMode.WRITE : AbstractStorageFile.OpenMode.READ;
        closer.defer(new AsyncFile(path, AbstractStorageFile.ReplaceMode.NORMAL, mode, key, key, false),
                replId, operation, cause);
    }

    public static void deferSegment(DeferredHandleCloser closer, String dir, String prefix, boolean write,
                                    ReplId replId, String operation, Throwable cause) {
        String key = StorageUtil.segmentKey(dir, prefix);
        closer.defer(new AsyncSegmentFile(dir, prefix, Collections.emptyList(), key, key, write),
                replId, operation, cause);
    }
}
