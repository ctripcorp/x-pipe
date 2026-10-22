package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.UncheckedIOException;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexWriterBlockReleaseTest {

    private static final String UUID = "a4f566ef50a85e1119f17f9b746728b48609a2ab";

    private AsyncFileSystem fs;
    private AsyncFile indexFile;
    private AsyncFile blockFile;
    private DefaultIndexStore store;
    private final AtomicBoolean failBlockWrite = new AtomicBoolean(false);

    @Before
    public void setUp() {
        fs = mock(AsyncFileSystem.class);
        AsyncCommandStore cmdStore = mock(AsyncCommandStore.class);
        when(cmdStore.getAsyncFileSystem()).thenReturn(fs);
        store = mock(DefaultIndexStore.class);
        when(store.getAsyncCommandStore()).thenReturn(cmdStore);
        indexFile = mock(AsyncFile.class);
        blockFile = mock(AsyncFile.class);

        when(fs.size(any(AsyncFile.class))).thenReturn(CompletableFuture.completedFuture(1L));
        when(fs.truncate(any(AsyncFile.class), anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        when(fs.write(any(AsyncFile.class), any(ByteBuf.class))).thenAnswer(inv -> {
            AsyncFile file = inv.getArgument(0);
            ByteBuf data = inv.getArgument(1);
            int length = data.readableBytes();
            data.release();
            if (file == blockFile && failBlockWrite.get()) {
                throw new UncheckedIOException(new IOException("inject block write fail"));
            }
            return CompletableFuture.completedFuture((long) length);
        });
    }

    @Test
    public void testV2FlushReleasesBlockAfterWrite() throws Exception {
        IndexWriterV2 writer = newV2Writer();
        writer.appendGtid(UUID, 1, 0, Collections.singletonList(10));
        ByteBuf cache = blockCache(currentBlock(writer));
        Assert.assertEquals(1, cache.refCnt());
        Assert.assertFalse(cache.isDirect());

        writer.flush();

        Assert.assertNull(currentBlock(writer));
        Assert.assertEquals(0, cache.refCnt());
    }

    @Test
    public void testV2GnoGapReleasesOldBlock() throws Exception {
        IndexWriterV2 writer = newV2Writer();
        writer.appendGtid(UUID, 1, 0, Collections.singletonList(10));
        BlockEntry oldBlock = currentBlock(writer);
        ByteBuf oldCache = blockCache(oldBlock);

        writer.appendGtid(UUID, 5, 10, Collections.singletonList(10));

        Assert.assertNotSame(oldBlock, currentBlock(writer));
        Assert.assertEquals(0, oldCache.refCnt());
        Assert.assertEquals(1, blockCache(currentBlock(writer)).refCnt());
    }

    @Test
    public void testV2WriteFailureKeepsBlockForRetry() throws Exception {
        IndexWriterV2 writer = newV2Writer();
        writer.appendGtid(UUID, 1, 0, Collections.singletonList(10));
        BlockEntry block = currentBlock(writer);
        ByteBuf cache = blockCache(block);

        failBlockWrite.set(true);
        try {
            writer.flush();
            Assert.fail("block write failure must propagate");
        } catch (UncheckedIOException expected) {
            Assert.assertEquals("inject block write fail", expected.getCause().getMessage());
        }
        Assert.assertSame(block, currentBlock(writer));
        Assert.assertEquals(1, cache.refCnt());
        Assert.assertTrue(block.getPendingBytes() > 0);

        failBlockWrite.set(false);
        writer.flush();

        Assert.assertNull(currentBlock(writer));
        Assert.assertEquals(0, cache.refCnt());
    }

    @Test
    public void testV2CloseReleasesBlockWithoutWriteAndRetiresWriter() throws Exception {
        IndexWriterV2 writer = newV2Writer();
        writer.appendGtid(UUID, 1, 0, Collections.singletonList(10));
        ByteBuf cache = blockCache(currentBlock(writer));
        clearInvocations(fs);

        writer.close();
        writer.close();

        Assert.assertNull(currentBlock(writer));
        Assert.assertEquals(0, cache.refCnt());
        verify(fs, never()).write(any(AsyncFile.class), any(ByteBuf.class));
        Assert.assertNotNull(writer.getGtidSet());
        assertClosed(() -> writer.appendGtid(UUID, 2, 10, Collections.singletonList(10)));
        assertClosed(() -> writer.appendNonGtid(20, Collections.singletonList(10)));
        assertClosed(writer::flush);
        assertClosed(() -> writer.init(indexFile, blockFile));
    }

    @Test
    public void testV1FlushReleasesBlockAfterWrite() throws Exception {
        IndexWriter writer = newV1Writer();
        writer.append(UUID, 1, 0);
        ByteBuf cache = blockCache(currentBlock(writer));
        Assert.assertEquals(1, cache.refCnt());
        Assert.assertFalse(cache.isDirect());

        writer.flush();

        Assert.assertNull(currentBlock(writer));
        Assert.assertEquals(0, cache.refCnt());
    }

    @Test
    public void testV1GnoGapReleasesOldBlock() throws Exception {
        IndexWriter writer = newV1Writer();
        writer.append(UUID, 1, 0);
        BlockEntry oldBlock = currentBlock(writer);
        ByteBuf oldCache = blockCache(oldBlock);

        writer.append(UUID, 5, 10);

        Assert.assertNotSame(oldBlock, currentBlock(writer));
        Assert.assertEquals(0, oldCache.refCnt());
        Assert.assertEquals(1, blockCache(currentBlock(writer)).refCnt());
    }

    @Test
    public void testV1WriteFailureKeepsBlockForRetry() throws Exception {
        IndexWriter writer = newV1Writer();
        writer.append(UUID, 1, 0);
        BlockEntry block = currentBlock(writer);
        ByteBuf cache = blockCache(block);

        failBlockWrite.set(true);
        try {
            writer.flush();
            Assert.fail("block write failure must propagate");
        } catch (UncheckedIOException expected) {
            Assert.assertEquals("inject block write fail", expected.getCause().getMessage());
        }
        Assert.assertSame(block, currentBlock(writer));
        Assert.assertEquals(1, cache.refCnt());

        failBlockWrite.set(false);
        writer.flush();

        Assert.assertNull(currentBlock(writer));
        Assert.assertEquals(0, cache.refCnt());
    }

    @Test
    public void testV1CloseReleasesBlockWithoutWriteAndRetiresWriter() throws Exception {
        IndexWriter writer = newV1Writer();
        writer.append(UUID, 1, 0);
        ByteBuf cache = blockCache(currentBlock(writer));
        clearInvocations(fs);

        writer.close();
        writer.close();

        Assert.assertNull(currentBlock(writer));
        Assert.assertEquals(0, cache.refCnt());
        verify(fs, never()).write(any(AsyncFile.class), any(ByteBuf.class));
        Assert.assertNotNull(writer.getGtidSet());
        assertClosed(() -> writer.append(UUID, 2, 10));
        assertClosed(writer::flush);
        assertClosed(writer::saveIndexEntry);
        assertClosed(() -> writer.init(indexFile, blockFile));
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws Exception;
    }

    private static void assertClosed(IoAction action) throws Exception {
        try {
            action.run();
            Assert.fail("closed writer must reject the call");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("closed"));
        }
    }

    private IndexWriterV2 newV2Writer() throws IOException {
        IndexWriterV2 writer = new IndexWriterV2(new GtidSet(""), store, 100, 16L * 1024 * 1024, 100);
        writer.init(indexFile, blockFile);
        return writer;
    }

    private IndexWriter newV1Writer() throws IOException {
        IndexWriter writer = new IndexWriter(new GtidSet(""), store);
        writer.init(indexFile, blockFile);
        return writer;
    }

    private static BlockEntry currentBlock(Object writer) {
        return (BlockEntry) ReflectionTestUtils.getField(writer, "currentBlock");
    }

    private static ByteBuf blockCache(BlockEntry block) {
        Assert.assertNotNull("writer has no current block", block);
        return (ByteBuf) ReflectionTestUtils.getField(block, "blockCache");
    }
}
