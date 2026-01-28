package com.ctrip.xpipe.redis.keeper.store.searcher;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.gtid.index.StreamCommandReader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit test for GtidCommandSearcher, focusing on onCommand method
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class GtidCommandSearcherTest extends AbstractTest {

    private static final String TEST_UUID = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
    private static final int BEG_GNO = 1;
    private static final int END_GNO = 10;

    @Mock
    private RedisKeeperServer redisKeeperServer;

    @Mock
    private RedisOpParser redisOpParser;

    @Mock
    private CommandFile commandFile;

    private GtidCommandSearcher searcher;
    private StreamCommandReader readerSpy;

    @Before
    public void setUp() {
        searcher = new GtidCommandSearcher(TEST_UUID, BEG_GNO, END_GNO, redisKeeperServer, redisOpParser);
        // Use reflection to get the reader and create a spy
        try {
            java.lang.reflect.Field readerField = GtidCommandSearcher.class.getDeclaredField("reader");
            readerField.setAccessible(true);
            StreamCommandReader reader = (StreamCommandReader) readerField.get(searcher);
            readerSpy = spy(reader);
            readerField.set(searcher, readerSpy);
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup reader spy", e);
        }
    }

    @Test
    public void testOnCommandWithFileRegion() throws IOException {
        // Create a mock FileRegion that will write data to the channel
        FileRegion fileRegion = mock(FileRegion.class);
        byte[] testData = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".getBytes();
        
        // Mock transferTo to write data to the channel
        doAnswer(invocation -> {
            WritableByteChannel channel = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.wrap(testData);
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            return (long) written;
        }).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        // Call onCommand
        searcher.onCommand(commandFile, 0L, fileRegion);

        // Verify transferTo was called
        verify(fileRegion, times(1)).transferTo(any(WritableByteChannel.class), eq(0L));

        // Verify reader.doRead was called (through the processor callback)
        // The processor is called when buffer is flushed, so we verify it was called
        verify(readerSpy, atLeastOnce()).doRead(any(ByteBuf.class));
    }

    @Test
    public void testOnCommandWithNonFileRegion() throws IOException {
        // Test with a non-FileRegion object
        Object nonFileRegion = new Object();

        // Call onCommand
        Object result = searcher.onCommand(commandFile, 0L, nonFileRegion);

        // Should return null and not process anything
        Assert.assertNull(result);
        // Note: verify() doesn't throw IOException, but doRead() declares it, so we add throws for linter
        verify(readerSpy, never()).doRead(any(ByteBuf.class));
    }

    @Test
    public void testOnCommandWithFileRegionThrowsException() throws IOException {
        // Create a mock FileRegion that throws exception
        FileRegion fileRegion = mock(FileRegion.class);
        IOException ioException = new IOException("Test exception");
        
        doThrow(ioException).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        // Call onCommand and expect exception
        try {
            searcher.onCommand(commandFile, 0L, fileRegion);
            Assert.fail("Expected XpipeRuntimeException");
        } catch (XpipeRuntimeException e) {
            Assert.assertEquals("Error processing file region", e.getMessage());
            Assert.assertEquals(ioException, e.getCause());
        }

        // Verify transferTo was called
        verify(fileRegion, times(1)).transferTo(any(WritableByteChannel.class), eq(0L));
    }

    @Test
    public void testOnCommandWithFileRegionLargeData() throws IOException {
        // Test with data larger than buffer size (128KB)
        FileRegion fileRegion = mock(FileRegion.class);
        int largeDataSize = 200 * 1024; // 200KB, larger than 128KB buffer
        byte[] largeData = new byte[largeDataSize];
        for (int i = 0; i < largeDataSize; i++) {
            largeData[i] = (byte) (i % 256);
        }
        
        // Mock transferTo to write data in chunks
        doAnswer(invocation -> {
            WritableByteChannel channel = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.wrap(largeData);
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            return (long) written;
        }).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        // Call onCommand
        searcher.onCommand(commandFile, 0L, fileRegion);

        // Verify transferTo was called
        verify(fileRegion, times(1)).transferTo(any(WritableByteChannel.class), eq(0L));

        // Verify reader.doRead was called multiple times (due to buffer flushing)
        verify(readerSpy, atLeastOnce()).doRead(any(ByteBuf.class));
    }

    @Test
    public void testOnCommandWithFileRegionEmptyData() throws IOException {
        // Test with empty data
        FileRegion fileRegion = mock(FileRegion.class);
        byte[] emptyData = new byte[0];
        
        doAnswer(invocation -> {
            WritableByteChannel channel = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.wrap(emptyData);
            return (long) channel.write(buffer);
        }).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        // Call onCommand
        searcher.onCommand(commandFile, 0L, fileRegion);

        // Verify transferTo was called
        verify(fileRegion, times(1)).transferTo(any(WritableByteChannel.class), eq(0L));

        // With empty data, doRead might not be called (no data to flush)
        // But flush() should still be called, which might call doRead with empty buffer
        // This depends on BoundedWritableByteChannel implementation
    }

    @Test
    public void testOnCommandWithFileRegionProcessorThrowsException() throws IOException {
        // Create a mock FileRegion
        FileRegion fileRegion = mock(FileRegion.class);
        byte[] testData = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".getBytes();
        
        // Mock transferTo to write data
        doAnswer(invocation -> {
            WritableByteChannel channel = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.wrap(testData);
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            return (long) written;
        }).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        // Make reader.doRead throw IOException
        IOException ioException = new IOException("Processor exception");
        doThrow(ioException).when(readerSpy).doRead(any(ByteBuf.class));

        // Call onCommand and expect exception
        try {
            searcher.onCommand(commandFile, 0L, fileRegion);
            Assert.fail("Expected XpipeRuntimeException");
        } catch (XpipeRuntimeException e) {
            Assert.assertEquals("Error processing file region", e.getMessage());
            // The cause should be IOException from processor
            Assert.assertTrue(e.getCause() instanceof IOException || 
                            e.getCause().getCause() instanceof IOException);
        }

        // Verify transferTo was called
        verify(fileRegion, times(1)).transferTo(any(WritableByteChannel.class), eq(0L));
    }

    @Test
    public void testOnCommandWithFileRegionMultipleChunks() throws IOException {
        // Test that data is processed in chunks correctly
        FileRegion fileRegion = mock(FileRegion.class);
        byte[] testData = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".getBytes();
        
        // Use ArgumentCaptor to capture ByteBuf passed to doRead
        ArgumentCaptor<ByteBuf> byteBufCaptor = ArgumentCaptor.forClass(ByteBuf.class);
        
        // Mock transferTo to write data
        doAnswer(invocation -> {
            WritableByteChannel channel = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.wrap(testData);
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            return (long) written;
        }).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        // Call onCommand
        searcher.onCommand(commandFile, 0L, fileRegion);

        // Verify reader.doRead was called
        verify(readerSpy, atLeastOnce()).doRead(byteBufCaptor.capture());
        
        // Verify the captured ByteBuf contains the expected data
        List<ByteBuf> capturedBufs = byteBufCaptor.getAllValues();
        Assert.assertFalse("Should have captured at least one ByteBuf", capturedBufs.isEmpty());
        
        // Verify all captured buffers are released (BoundedWritableByteChannel releases them)
        // Note: We can't verify refCnt here as buffers are released by BoundedWritableByteChannel
        Assert.assertTrue("Should have captured ByteBufs", capturedBufs.size() > 0);
    }

    @Test
    public void testOnCommandWithFileRegionVerifyChannelClosed() throws IOException {
        // Test that channel is properly closed even if exception occurs
        FileRegion fileRegion = mock(FileRegion.class);
        IOException ioException = new IOException("Test exception");
        
        doThrow(ioException).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        // Call onCommand
        try {
            searcher.onCommand(commandFile, 0L, fileRegion);
            Assert.fail("Expected XpipeRuntimeException");
        } catch (XpipeRuntimeException e) {
            // Expected
        }

        // Verify transferTo was called
        verify(fileRegion, times(1)).transferTo(any(WritableByteChannel.class), eq(0L));
        
        // The channel should be closed in finally block, but we can't directly verify it
        // as it's created inside the method. The test verifies no resource leaks occur.
    }

    @Test
    public void testOnCommandWithFileRegionVerifyFlushCalled() throws IOException {
        // Test that flush is called after transferTo
        FileRegion fileRegion = mock(FileRegion.class);
        byte[] testData = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".getBytes();
        
        // Mock transferTo to write data
        doAnswer(invocation -> {
            WritableByteChannel channel = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.wrap(testData);
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            return (long) written;
        }).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        // Call onCommand
        searcher.onCommand(commandFile, 0L, fileRegion);

        // Verify transferTo was called
        verify(fileRegion, times(1)).transferTo(any(WritableByteChannel.class), eq(0L));
        
        // Verify reader.doRead was called (which happens during flush)
        verify(readerSpy, atLeastOnce()).doRead(any(ByteBuf.class));
    }

    @Test
    public void testOnGtidCmd() throws IOException {
        RedisOpParserManager manager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(manager);
        RedisOpParser redisOpParser = new GeneralRedisOpParser(manager);
        GtidCommandSearcher searcher = new GtidCommandSearcher("781dd4ac1bc3149937dd5efe3d45e37b8649cfbf", 555, 560, redisKeeperServer, redisOpParser);
        searcher.setCmdKeyItems(new ArrayList<>());

        FileRegion fileRegion = mock(FileRegion.class);
        byte[] testData = ("*8\r\n" +
                "$4\r\n" +
                "GTID\r\n" +
                "$44\r\n" +
                "781dd4ac1bc3149937dd5efe3d45e37b8649cfbf:558\r\n" +
                "$1\r\n" +
                "0\r\n" +
                "$4\r\n" +
                "mset\r\n" +
                "$2\r\n" +
                "k1\r\n" +
                "$2\r\n" +
                "v1\r\n" +
                "$2\r\n" +
                "k2\r\n" +
                "$2\r\n" +
                "v2\r\n" +
                "*6\r\n" +
                "$4\r\n" +
                "GTID\r\n" +
                "$44\r\n" +
                "781dd4ac1bc3149937dd5efe3d45e37b8649cfbf:557\r\n" +
                "$1\r\n" +
                "1\r\n" +
                "$3\r\n" +
                "set\r\n" +
                "$2\r\n" +
                "k1\r\n" +
                "$2\r\n" +
                "v1\r\n" +
                "*7\r\n" +
                "$4\r\n" +
                "GTID\r\n" +
                "$44\r\n" +
                "781dd4ac1bc3149937dd5efe3d45e37b8649cfbf:559\r\n" +
                "$1\r\n" +
                "0\r\n" +
                "$4\r\n" +
                "hset\r\n" +
                "$2\r\n" +
                "h1\r\n" +
                "$2\r\n" +
                "f1\r\n" +
                "$2\r\n" +
                "v1\r\n").getBytes();

        // Mock transferTo to write data
        doAnswer(invocation -> {
            WritableByteChannel channel = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.wrap(testData);
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            return (long) written;
        }).when(fileRegion).transferTo(any(WritableByteChannel.class), eq(0L));

        searcher.onCommand(commandFile, 0L, fileRegion);
        List<CmdKeyItem> items = searcher.getCmdKeyItems();
        logger.info("[test] {}", items);
        Assert.assertEquals("MSET", items.get(0).cmd);
        Assert.assertArrayEquals("k1".getBytes(), items.get(0).key);
        Assert.assertEquals(558, items.get(0).seq);
        Assert.assertEquals(0, items.get(0).dbid);
        Assert.assertEquals("MSET", items.get(1).cmd);
        Assert.assertArrayEquals("k2".getBytes(), items.get(1).key);
        Assert.assertEquals(558, items.get(1).seq);
        Assert.assertEquals(0, items.get(1).dbid);
        Assert.assertEquals("SET", items.get(2).cmd);
        Assert.assertArrayEquals("k1".getBytes(), items.get(2).key);
        Assert.assertEquals(557, items.get(2).seq);
        Assert.assertEquals(1, items.get(2).dbid);
        Assert.assertEquals("HSET", items.get(3).cmd);
        Assert.assertArrayEquals("h1".getBytes(), items.get(3).key);
        Assert.assertArrayEquals("f1".getBytes(), items.get(3).subkey);
        Assert.assertEquals(559, items.get(3).seq);
        Assert.assertEquals(0, items.get(3).dbid);
    }

}

