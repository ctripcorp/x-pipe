package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.withSettings;

@RunWith(MockitoJUnitRunner.class)
public class OffsetCommandReaderTest {

    OffsetCommandReader reader;

    private CommandStore commandStore;

    private AsyncCommandStore asyncCommandStore;

    @Mock
    private AsyncFileSystem asyncFileSystem;

    @Mock
    private AsyncSegmentFile readAsyncSegmentFile;

    @Mock
    private OffsetNotifier notifier;

    @Mock
    private ReplDelayConfig config;

    @Before
    public void setupOffsetCommandReaderTest() throws Exception {
        commandStore = Mockito.mock(CommandStore.class,
                withSettings().extraInterfaces(AsyncCommandStore.class));
        asyncCommandStore = (AsyncCommandStore) commandStore;

        Mockito.when(asyncCommandStore.getAsyncFileSystem()).thenReturn(asyncFileSystem);
        Mockito.when(asyncCommandStore.getCommandBaseDir()).thenReturn(new java.io.File("/tmp"));
        Mockito.when(asyncCommandStore.getCommandFileNamePrefix()).thenReturn("cmd_");
        Mockito.when(asyncCommandStore.getCommandIndexPrefixes()).thenReturn(Collections.emptyList());
        Mockito.when(asyncCommandStore.getFileSystemReplId()).thenReturn(ReplId.from(1L));
        Mockito.when(asyncFileSystem.open(Mockito.anyString(), Mockito.anyString(), Mockito.anyList(), Mockito.eq(false), Mockito.eq("repl_1")))
                .thenReturn(CompletableFuture.completedFuture(readAsyncSegmentFile));
        Mockito.when(commandStore.totalLength()).thenReturn(200L);
        Mockito.when(config.getPsyncLimitPerSecond()).thenReturn(-1);
    }

    @Test
    public void testReadToWall() throws Exception {
        reader = new OffsetCommandReader(1, 101, commandStore, notifier, config, 1024);

        ReferenceFileRegion region = reader.doRead(10);
        Assert.assertEquals(100, region.count());

        region = reader.doRead(10);
        Assert.assertEquals(ReferenceFileRegion.EOF, region);
    }

    @Test
    public void testGetReadOffsetTracksTransferToNotScanCursor() throws Exception {
        // Limit each emit to 100 bytes so we can have two in-flight regions.
        Mockito.when(config.getPsyncLimitPerSecond()).thenReturn(100);
        reader = new OffsetCommandReader(10, -1, commandStore, notifier, config, 1024);
        Assert.assertEquals(10, reader.getReadOffset());

        WritableByteChannel sink = new WritableByteChannel() {
            @Override
            public int write(ByteBuffer src) {
                int n = src.remaining();
                src.position(src.limit());
                return n;
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() {
            }
        };

        // Each Netty-style transferTo: one partial FS transfer then 0 to stop the retry loop.
        java.util.concurrent.atomic.AtomicLong nextTransferBytes =
                new java.util.concurrent.atomic.AtomicLong(0);
        Mockito.when(asyncFileSystem.transferTo(
                        Mockito.eq(readAsyncSegmentFile), Mockito.anyLong(), Mockito.anyLong(), Mockito.eq(sink)))
                .thenAnswer(invocation -> {
                    long requested = invocation.getArgument(2);
                    long allow = nextTransferBytes.getAndSet(0);
                    if (allow <= 0) {
                        return CompletableFuture.completedFuture(0L);
                    }
                    return CompletableFuture.completedFuture(Math.min(allow, requested));
                });

        // region1 [10, 110)
        AsyncReferenceFileRegion region1 = (AsyncReferenceFileRegion) reader.doRead(10);
        Assert.assertEquals(100, region1.count());
        // scan cursor advanced to 110, transferTo not yet called
        Assert.assertEquals(10, reader.getReadOffset());

        // region2 [110, 200)
        AsyncReferenceFileRegion region2 = (AsyncReferenceFileRegion) reader.doRead(10);
        Assert.assertEquals(90, region2.count());
        Assert.assertEquals(10, reader.getReadOffset());

        nextTransferBytes.set(40);
        Assert.assertEquals(40, region1.transferTo(sink, 0));
        Assert.assertEquals(50, reader.getReadOffset());

        nextTransferBytes.set(60);
        Assert.assertEquals(60, region1.transferTo(sink, 40));
        Assert.assertEquals(110, reader.getReadOffset());

        reader.flushed(region1);
        // next flying region not transferred yet
        Assert.assertEquals(110, reader.getReadOffset());

        nextTransferBytes.set(90);
        Assert.assertEquals(90, region2.transferTo(sink, 0));
        Assert.assertEquals(200, reader.getReadOffset());

        reader.flushed(region2);
        Assert.assertEquals(200, reader.getReadOffset());
    }

    @Test
    public void testCloseReleasesReadSegment() throws Exception {
        reader = new OffsetCommandReader(1, 101, commandStore, notifier, config, 1024);
        Mockito.when(asyncFileSystem.close(readAsyncSegmentFile))
                .thenReturn(CompletableFuture.completedFuture(null));

        reader.close();

        Mockito.verify(asyncFileSystem).close(readAsyncSegmentFile);
        Mockito.verify(commandStore).removeReader(reader);
    }

}
