package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Verifies RDB read-handle close is deferred until in-flight FileRegions deallocate
 * (Netty async writeAndFlush / transferTo race).
 */
public class AsyncRdbReadHandleTest extends AbstractRedisKeeperTest {

    @Test
    public void closeDefersUntilRegionDeallocated() throws Exception {
        AsyncFileSystem fs = asyncFileSystem();
        File path = new File(getTestFileDir(), getTestName() + ".rdb");
        AsyncFile writeFile = AsyncFileSystemHelper.await(
                fs.open(path.getAbsolutePath(), AbstractStorageFile.OpenMode.WRITE, false, true, getReplId().toString()),
                "open write");
        try {
            AsyncFileSystemHelper.writeAndAwait(fs, writeFile, io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4}),
                    4, "write rdb");
            AsyncFileSystemHelper.await(fs.fsync(writeFile), "fsync");
        } finally {
            AsyncFileSystemHelper.await(fs.close(writeFile), "close write");
        }

        AsyncFile readFile = AsyncFileSystemHelper.await(
                fs.open(path.getAbsolutePath(), AbstractStorageFile.OpenMode.READ, false, true, getReplId().toString()),
                "open read");
        AsyncRdbReadHandle handle = new AsyncRdbReadHandle(fs, readFile, path.getAbsolutePath());
        AsyncRdbReferenceFileRegion region = handle.read(0, 4);

        handle.close();
        Assert.assertTrue(handle.isMarkedClosed());
        Assert.assertFalse("file must stay open while region pending", handle.isFileClosed());
        Assert.assertEquals(1, handle.pendingRefs());

        WritableByteChannel sink = new WritableByteChannel() {
            private boolean open = true;

            @Override
            public int write(ByteBuffer src) {
                int n = src.remaining();
                src.position(src.limit());
                return n;
            }

            @Override
            public boolean isOpen() {
                return open;
            }

            @Override
            public void close() {
                open = false;
            }
        };
        Assert.assertEquals(4, region.transferTo(sink, 0));

        region.deallocate();
        Assert.assertTrue(region.isDeallocated());
        Assert.assertTrue("file closes after last region deallocate", handle.isFileClosed());
        Assert.assertEquals(0, handle.pendingRefs());
    }

    @Test
    public void closeWithNoRegionClosesImmediately() throws Exception {
        AsyncFileSystem fs = asyncFileSystem();
        File path = new File(getTestFileDir(), getTestName() + "-empty.rdb");
        AsyncFile writeFile = AsyncFileSystemHelper.await(
                fs.open(path.getAbsolutePath(), AbstractStorageFile.OpenMode.WRITE, false, true, getReplId().toString()),
                "open write");
        AsyncFileSystemHelper.await(fs.close(writeFile), "close write");

        AsyncFile readFile = AsyncFileSystemHelper.await(
                fs.open(path.getAbsolutePath(), AbstractStorageFile.OpenMode.READ, false, true, getReplId().toString()),
                "open read");
        AsyncRdbReadHandle handle = new AsyncRdbReadHandle(fs, readFile, path.getAbsolutePath());
        handle.close();
        Assert.assertTrue(handle.isFileClosed());
        Assert.assertEquals(0, handle.pendingRefs());
    }
}
