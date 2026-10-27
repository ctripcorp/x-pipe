package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncRdbReferenceFileRegionTest extends AbstractTest {

    private static final long MAX = AsyncFileSystemHelper.TRANSFER_MAX_BYTES;

    private static final long FILE_POSITION = 100;

    private AsyncFileSystem fs;

    private AsyncFile file;

    private WritableByteChannel target;

    private final List<long[]> calls = new ArrayList<>();

    private final Deque<Long> accepted = new ArrayDeque<>();

    @Before
    public void beforeAsyncRdbReferenceFileRegionTest() {
        fs = mock(AsyncFileSystem.class);
        file = mock(AsyncFile.class);
        target = mock(WritableByteChannel.class);
        when(fs.transferTo(any(AsyncFile.class), anyLong(), anyLong(), any(WritableByteChannel.class)))
                .thenAnswer(invocation -> {
                    long position = invocation.getArgument(1);
                    long count = invocation.getArgument(2);
                    calls.add(new long[]{position, count});
                    Long n = accepted.poll();
                    return CompletableFuture.completedFuture(n == null ? count : Math.min(n, count));
                });
    }

    private AsyncRdbReferenceFileRegion region(long count) {
        return new AsyncRdbReadHandle(fs, file, "test", null).read(FILE_POSITION, count);
    }

    private void assertCall(int index, long position, long count) {
        Assert.assertEquals("position of call " + index, position, calls.get(index)[0]);
        Assert.assertEquals("count of call " + index, count, calls.get(index)[1]);
    }

    @Test
    public void smallRegionTransfersInOneCall() throws IOException {
        AsyncRdbReferenceFileRegion region = region(10);

        Assert.assertEquals(10, region.transferTo(target, 0));
        Assert.assertEquals(1, calls.size());
        assertCall(0, FILE_POSITION, 10);
        Assert.assertEquals(10, region.transferred());
    }

    @Test
    public void largeRegionIsSplitByLimit() throws IOException {
        long count = 2 * MAX + MAX / 2;
        AsyncRdbReferenceFileRegion region = region(count);

        Assert.assertEquals(count, region.transferTo(target, 0));
        Assert.assertEquals(3, calls.size());
        assertCall(0, FILE_POSITION, MAX);
        assertCall(1, FILE_POSITION + MAX, MAX);
        assertCall(2, FILE_POSITION + 2 * MAX, MAX / 2);
        Assert.assertEquals(count, region.transferred());
    }

    @Test
    public void resumesFromNettyPosition() throws IOException {
        AsyncRdbReferenceFileRegion region = region(MAX + 10);

        Assert.assertEquals(MAX + 5, region.transferTo(target, 5));
        Assert.assertEquals(2, calls.size());
        assertCall(0, FILE_POSITION + 5, MAX);
        assertCall(1, FILE_POSITION + 5 + MAX, 5);
        Assert.assertEquals(MAX + 10, region.transferred());
    }

    @Test
    public void partialTransferContinuesAndZeroStops() throws IOException {
        accepted.add(3L);
        accepted.add(0L);
        AsyncRdbReferenceFileRegion region = region(10);

        Assert.assertEquals(3, region.transferTo(target, 0));
        Assert.assertEquals(2, calls.size());
        assertCall(0, FILE_POSITION, 10);
        assertCall(1, FILE_POSITION + 3, 7);
        Assert.assertEquals(3, region.transferred());
    }

    @Test
    public void timeoutStillThrows() {
        when(fs.transferTo(any(AsyncFile.class), anyLong(), anyLong(), any(WritableByteChannel.class)))
                .thenAnswer(invocation -> {
                    calls.add(new long[]{invocation.getArgument(1), invocation.getArgument(2)});
                    return new CompletableFuture<Long>();
                });
        AsyncRdbReferenceFileRegion region = region(3 * MAX);

        try {
            AsyncFileSystemHelper.runWithIoTimeout(50, () -> region.transferTo(target, 0));
            Assert.fail("expected timeout");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("timeout"));
        }
        Assert.assertEquals(1, calls.size());
        assertCall(0, FILE_POSITION, MAX);
        Assert.assertEquals(0, region.transferred());
    }
}
