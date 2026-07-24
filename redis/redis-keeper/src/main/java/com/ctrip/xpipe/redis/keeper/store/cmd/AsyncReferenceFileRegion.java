package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import io.netty.channel.FileRegion;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncReferenceFileRegion implements ReferenceFileRegion {

    private final AsyncFileSystem asyncFileSystem;

    private final AsyncSegmentFile asyncSegmentFile;

    private final long logicalPosition;

    private final long count;

    private final AtomicInteger refCnt = new AtomicInteger(1);

    private final AtomicBoolean deallocated = new AtomicBoolean(false);

    private volatile long transferred;

    private volatile long totalPos;

    public AsyncReferenceFileRegion(AsyncFileSystem asyncFileSystem, AsyncSegmentFile asyncSegmentFile,
                                    long logicalPosition, long count) {
        this.asyncFileSystem = asyncFileSystem;
        this.asyncSegmentFile = asyncSegmentFile;
        this.logicalPosition = logicalPosition;
        this.count = count;
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

        long wrote = 0;
        while (remaining > 0) {
            long transferredOnce = AsyncFileSystemHelper.await(
                    asyncFileSystem.transferTo(asyncSegmentFile, logicalPosition + position + wrote, remaining, target),
                    "transfer command segment " + logicalPosition);
            if (transferredOnce <= 0) {
                break;
            }
            wrote += transferredOnce;
            remaining -= transferredOnce;
        }
        transferred = Math.max(transferred, position + wrote);
        return wrote;
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
