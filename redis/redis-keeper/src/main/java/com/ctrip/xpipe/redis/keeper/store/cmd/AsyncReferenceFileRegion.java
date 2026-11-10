package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.storage.TransferToDelayAware;
import com.ctrip.xpipe.redis.keeper.storage.TransferToDelayContext;
import com.ctrip.xpipe.redis.keeper.storage.TransferToDelayHook;
import io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncReferenceFileRegion implements ReferenceFileRegion, TransferToDelayAware {

    private final AsyncFileSystem asyncFileSystem;

    private final AsyncSegmentFile asyncSegmentFile;

    private final long logicalPosition;

    private final long count;

    private final AtomicInteger refCnt = new AtomicInteger(1);

    private final AtomicBoolean deallocated = new AtomicBoolean(false);

    private volatile long transferred;

    private volatile long totalPos;

    private volatile TransferToDelayHook transferToDelayHook = TransferToDelayHook.NOOP;

    public AsyncReferenceFileRegion(AsyncFileSystem asyncFileSystem, AsyncSegmentFile asyncSegmentFile,
                                    long logicalPosition, long count) {
        this.asyncFileSystem = asyncFileSystem;
        this.asyncSegmentFile = asyncSegmentFile;
        this.logicalPosition = logicalPosition;
        this.count = count;
    }

    @Override
    public void bindTransferToDelayHook(TransferToDelayHook hook) {
        this.transferToDelayHook = hook == null ? TransferToDelayHook.NOOP : hook;
    }

    @Override
    public void deallocate() {
        deallocated.compareAndSet(false, true);
    }

    @Override
    public long getTotalPos() {
        return totalPos;
    }

    @Override
    public void setTotalPos(long totalPos) {
        this.totalPos = totalPos;
    }

    @Override
    public long position() {
        return logicalPosition;
    }

    @Override
    public long transfered() {
        return transferred();
    }

    @Override
    public long transferred() {
        return transferred;
    }

    /**
     * Absolute logical offset already delivered by {@link #transferTo}.
     * Equals {@code logicalPosition} before any transfer; advances as Netty drains this region.
     * Used by {@link OffsetCommandReader#getReadOffset()} as the GC lowest-read gate.
     */
    public long getCurrentReadOffset() {
        return logicalPosition + transferred;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        long remaining = count - position;
        if (remaining <= 0) {
            return 0;
        }

        long transferPosition = logicalPosition + position;
        long transferOffset = transferPosition + remaining;
        TransferToDelayContext delayContext = TransferToDelayHook.safeBegin(
                transferToDelayHook, transferPosition, transferOffset);
        long wrote = 0;
        Throwable failure = null;
        try {
            while (remaining > 0) {
                final long transferredSoFar = wrote;
                final long requestBytes = Math.min(remaining, AsyncFileSystemHelper.TRANSFER_MAX_BYTES);
                long transferredOnce = AsyncFileSystemHelper.await(() -> {
                    long offset = transferPosition + transferredSoFar;
                    if (delayContext == TransferToDelayContext.NONE) {
                        return asyncFileSystem.transferTo(asyncSegmentFile, offset, requestBytes, target);
                    }
                    return asyncFileSystem.transferTo(asyncSegmentFile, offset, requestBytes, target, delayContext);
                }, "transfer command segment " + logicalPosition);
                if (transferredOnce <= 0) {
                    break;
                }
                wrote += transferredOnce;
                remaining -= transferredOnce;
            }
            transferred = Math.max(transferred, position + wrote);
            return wrote;
        } catch (IOException | RuntimeException | Error t) {
            failure = t;
            throw t;
        } finally {
            TransferToDelayContext.safeComplete(delayContext, wrote, failure);
        }
    }

    @Override
    public FileRegion retain() {
        refCnt.incrementAndGet();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        refCnt.addAndGet(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        return this;
    }

    @Override
    public int refCnt() {
        return refCnt.get();
    }

    @Override
    public boolean release() {
        return release(1);
    }

    @Override
    public boolean release(int decrement) {
        int value = refCnt.addAndGet(-decrement);
        if (value <= 0) {
            deallocate();
            return true;
        }
        return false;
    }
}
