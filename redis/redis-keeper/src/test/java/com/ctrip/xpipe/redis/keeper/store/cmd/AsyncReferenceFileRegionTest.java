package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
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

public class AsyncReferenceFileRegionTest extends AbstractTest {

    private static final long MAX = AsyncFileSystemHelper.TRANSFER_MAX_BYTES;

    private static final long LOGICAL_POSITION = 1000;

    private AsyncFileSystem fs;

    private AsyncSegmentFile segmentFile;

    private WritableByteChannel target;

    private final List<long[]> calls = new ArrayList<>();

    private final Deque<Long> accepted = new ArrayDeque<>();

    @Before
    public void beforeAsyncReferenceFileRegionTest() {
        fs = mock(AsyncFileSystem.class);
        segmentFile = mock(AsyncSegmentFile.class);
        target = mock(WritableByteChannel.class);
        when(fs.transferTo(any(AsyncSegmentFile.class), anyLong(), anyLong(), any(WritableByteChannel.class)))
                .thenAnswer(invocation -> {
                    long offset = invocation.getArgument(1);
                    long count = invocation.getArgument(2);
                    calls.add(new long[]{offset, count});
                    Long n = accepted.poll();
                    return CompletableFuture.completedFuture(n == null ? count : Math.min(n, count));
                });
    }

    private AsyncReferenceFileRegion region(long count) {
        return new AsyncReferenceFileRegion(fs, segmentFile, LOGICAL_POSITION, count);
    }

    private void assertCall(int index, long offset, long count) {
        Assert.assertEquals("offset of call " + index, offset, calls.get(index)[0]);
        Assert.assertEquals("count of call " + index, count, calls.get(index)[1]);
    }

    @Test
    public void smallRegionTransfersInOneCall() throws IOException {
        AsyncReferenceFileRegion region = region(10);

        Assert.assertEquals(10, region.transferTo(target, 0));
        Assert.assertEquals(1, calls.size());
        assertCall(0, LOGICAL_POSITION, 10);
        Assert.assertEquals(10, region.transferred());
    }

    @Test
    public void largeRegionIsSplitByLimit() throws IOException {
        long count = 2 * MAX + MAX / 2;
        AsyncReferenceFileRegion region = region(count);

        Assert.assertEquals(count, region.transferTo(target, 0));
        Assert.assertEquals(3, calls.size());
        assertCall(0, LOGICAL_POSITION, MAX);
        assertCall(1, LOGICAL_POSITION + MAX, MAX);
        assertCall(2, LOGICAL_POSITION + 2 * MAX, MAX / 2);
        Assert.assertEquals(count, region.transferred());
    }

    @Test
    public void partialTransferContinuesAndZeroStops() throws IOException {
        accepted.add(MAX - 1);
        accepted.add(0L);
        AsyncReferenceFileRegion region = region(2 * MAX);

        Assert.assertEquals(MAX - 1, region.transferTo(target, 0));
        Assert.assertEquals(2, calls.size());
        assertCall(0, LOGICAL_POSITION, MAX);
        assertCall(1, LOGICAL_POSITION + MAX - 1, MAX);
        Assert.assertEquals(MAX - 1, region.transferred());
    }

    @Test
    public void timeoutStillThrows() {
        when(fs.transferTo(any(AsyncSegmentFile.class), anyLong(), anyLong(), any(WritableByteChannel.class)))
                .thenAnswer(invocation -> {
                    calls.add(new long[]{invocation.getArgument(1), invocation.getArgument(2)});
                    return new CompletableFuture<Long>();
                });
        AsyncReferenceFileRegion region = region(3 * MAX);

        try {
            AsyncFileSystemHelper.runWithIoTimeout(50, () -> region.transferTo(target, 0));
            Assert.fail("expected timeout");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("timeout"));
        }
        Assert.assertEquals(1, calls.size());
        assertCall(0, LOGICAL_POSITION, MAX);
        Assert.assertEquals(0, region.transferred());
    }
}
