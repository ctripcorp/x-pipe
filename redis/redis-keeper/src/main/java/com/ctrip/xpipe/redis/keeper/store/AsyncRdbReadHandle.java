package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RDB 读句柄生命周期：对齐旧 {@link com.ctrip.xpipe.netty.filechannel.ReferenceFileChannel}。
 * {@link #close()} 只标记关闭；底层 {@link AsyncFile} 在 closed 且无未完成 {@link AsyncRdbReferenceFileRegion}
 * 时才真正 {@code fs.close}，避免 Netty 异步 {@code transferTo} 打到已关闭 channel。
 */
public class AsyncRdbReadHandle implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRdbReadHandle.class);

    private final AsyncFileSystem asyncFileSystem;

    private final AsyncFile asyncFile;

    private final String tag;

    private final AtomicLong reference = new AtomicLong();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicBoolean fileClosed = new AtomicBoolean(false);

    public AsyncRdbReadHandle(AsyncFileSystem asyncFileSystem, AsyncFile asyncFile, String tag) {
        this.asyncFileSystem = asyncFileSystem;
        this.asyncFile = asyncFile;
        this.tag = tag;
    }

    public AsyncFileSystem getAsyncFileSystem() {
        return asyncFileSystem;
    }

    public AsyncFile getAsyncFile() {
        return asyncFile;
    }

    public AsyncRdbReferenceFileRegion read(long filePosition, long count) {
        reference.incrementAndGet();
        return new AsyncRdbReferenceFileRegion(this, filePosition, count);
    }

    @Override
    public void close() {
        closed.set(true);
        tryCloseFile();
    }

    void release() {
        long current = reference.decrementAndGet();
        if (current < 0) {
            logger.error("[release][current < 0]{}, {}", tag, current);
        }
        tryCloseFile();
    }

    private void tryCloseFile() {
        if (!closed.get() || reference.get() > 0) {
            return;
        }
        if (!fileClosed.compareAndSet(false, true)) {
            return;
        }
        try {
            logger.info("[tryCloseFile][doClose]{}", tag);
            AsyncFileSystemHelper.await(asyncFileSystem.close(asyncFile), "close rdb read " + tag);
        } catch (IOException e) {
            logger.error("[tryCloseFile]" + tag, e);
        }
    }

    boolean isMarkedClosed() {
        return closed.get();
    }

    boolean isFileClosed() {
        return fileClosed.get();
    }

    long pendingRefs() {
        return reference.get();
    }

    @Override
    public String toString() {
        return String.format("AsyncRdbReadHandle(%s, refs:%d, closed:%s, fileClosed:%s)",
                tag, reference.get(), closed.get(), fileClosed.get());
    }
}
