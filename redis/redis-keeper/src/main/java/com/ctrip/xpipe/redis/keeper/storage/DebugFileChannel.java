package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

import com.ctrip.xpipe.redis.keeper.storage.FileFaultInjector.Category;

/**
 * FileChannel of a debug-mode keeper (keeper-tfs-m6 §17.3): forwards every call to the real
 * channel, injecting faults first on reads (read / transferTo), writes (write / transferFrom /
 * truncate) and force. position / size / map / lock and close are not injected; transferTo goes
 * to the real channel, so sendfile stays zero copy.
 *
 * isOpen() is final in AbstractInterruptibleChannel and reflects this wrapper only: if the real
 * channel is closed by an interrupt, isOpen() stays true while every operation fails with
 * ClosedChannelException from the real channel. Nothing in storage relies on isOpen().
 */
class DebugFileChannel extends FileChannel {

    private final FileChannel delegate;
    private final String path;
    private final FileFaultInjector injector;

    DebugFileChannel(FileChannel delegate, String path, FileFaultInjector injector) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.path = path;
        this.injector = Objects.requireNonNull(injector, "injector");
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        injector.inject(Category.READ, path);
        return delegate.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        injector.inject(Category.READ, path);
        return delegate.read(dsts, offset, length);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        injector.inject(Category.READ, path);
        return delegate.read(dst, position);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        injector.inject(Category.READ, path);
        return delegate.transferTo(position, count, target);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        injector.inject(Category.WRITE, path);
        return delegate.write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        injector.inject(Category.WRITE, path);
        return delegate.write(srcs, offset, length);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        injector.inject(Category.WRITE, path);
        return delegate.write(src, position);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        injector.inject(Category.WRITE, path);
        return delegate.transferFrom(src, position, count);
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        injector.inject(Category.WRITE, path);
        delegate.truncate(size);
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        injector.inject(Category.FORCE, path);
        delegate.force(metaData);
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        delegate.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return delegate.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return delegate.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        delegate.close();
    }

    @Override
    public String toString() {
        return "DebugFileChannel(" + path + ")";
    }
}
