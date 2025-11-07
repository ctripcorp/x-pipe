package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class StreamCommandReaderTest {

    @Mock
    private DefaultIndexStore defaultIndexStore;

    private StreamCommandReader streamCommandReader;
    private List<ByteBuf> capturedByteBufs;
    private List<String> capturedGtids;
    private List<Long> capturedOffsets;

    private ArrayParser parser;

    @Before
    public void setUp() throws Exception {
        streamCommandReader = new StreamCommandReader(defaultIndexStore, 0);
        parser = new ArrayParser();
        capturedByteBufs = new ArrayList<>();
        capturedGtids = new ArrayList<>();
        capturedOffsets = new ArrayList<>();

        // Setup mock to capture parameters
        doAnswer(invocation -> {
            String gtid = invocation.getArgument(0);
            Long offset = invocation.getArgument(1);
            capturedGtids.add(gtid);
            capturedOffsets.add(offset);
            return true; // Return true to indicate index was written
        }).when(defaultIndexStore).onCommand(anyString(), anyLong());

        doAnswer(invocation -> {
            ByteBuf buf = invocation.getArgument(0);
            if (buf != null) {
                // Retain to track it
                ByteBuf captured = buf.slice();
                capturedByteBufs.add(captured);
            }
            return null;
        }).when(defaultIndexStore).onFinishParse(any(ByteBuf.class));
    }

    @After
    public void tearDown() {
        // Release all captured ByteBufs
        for (ByteBuf buf : capturedByteBufs) {
            if (buf != null && buf.refCnt() > 0) {
                buf.release();
            }
        }
        capturedByteBufs.clear();
    }

    @Test
    public void testRegularCommand() throws IOException {
        // Test regular SET command: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
        ByteBuf commandBuf = createRedisArrayCommand("SET", "key", "value");
        int initialRefCnt = commandBuf.refCnt();
        
        // Create payload for regular command
        Object[] payload = createCommandPayload("SET", "key", "value");
        
        // Process command
        streamCommandReader.onCommand(payload, commandBuf);
        
        // Verify onFinishParse was called
        verify(defaultIndexStore, times(1)).onFinishParse(any(ByteBuf.class));
        verify(defaultIndexStore, never()).onCommand(anyString(), anyLong());
        
        // Verify ByteBuf was properly handled (should not be released by reader, caller's responsibility)
        Assert.assertEquals("ByteBuf refCnt should remain unchanged", initialRefCnt, commandBuf.refCnt());
    }

    @Test
    public void testGtidCommand() throws IOException {
        String gtid = "a4f566ef50a85e1119f17f9b746728b48609a2ab:1";
        
        // Create GTID command: *2\r\n$4\r\nGTID\r\n$40\r\n<gtid>\r\n
        ByteBuf commandBuf = createRedisArrayCommand("GTID", gtid, "0", "SET", "key", "value");
        int initialRefCnt = commandBuf.refCnt();
        
        // Create payload for GTID command
        Object[] payload = createCommandPayload("GTID", gtid, "0", "SET", "key", "value");
        
        // Process command
        streamCommandReader.onCommand(payload, commandBuf);
        
        // Verify onCommand was called with GTID
        verify(defaultIndexStore, times(1)).onCommand(eq(gtid), anyLong());
        verify(defaultIndexStore, times(1)).onFinishParse(any(ByteBuf.class));
        
        // Verify GTID was captured
        Assert.assertEquals(1, capturedGtids.size());
        Assert.assertEquals(gtid, capturedGtids.get(0));
        
        // Verify ByteBuf was properly handled
        Assert.assertEquals("ByteBuf refCnt should remain unchanged", initialRefCnt, commandBuf.refCnt());
    }

    @Test
    public void testTransaction() throws IOException {
        String gtid = "a4f566ef50a85e1119f17f9b746728b48609a2ab:2";
        
        // Step 1: MULTI command
        ByteBuf multiBuf = createRedisArrayCommand("MULTI");
        Object[] multiPayload = createCommandPayload("MULTI");
        streamCommandReader.onCommand(multiPayload, multiBuf);
        
        // Verify transaction is active
        Assert.assertTrue("Transaction should be active", streamCommandReader.isTransactionActive());
        Assert.assertEquals(1, streamCommandReader.getTransactionSize());
        
        // Step 2: Regular command in transaction
        ByteBuf setBuf = createRedisArrayCommand("SET", "key1", "value1");
        Object[] setPayload = createCommandPayload("SET", "key1", "value1");
        streamCommandReader.onCommand(setPayload, setBuf);
        
        // Verify command was added to transaction
        Assert.assertTrue("Transaction should still be active", streamCommandReader.isTransactionActive());
        Assert.assertEquals(2, streamCommandReader.getTransactionSize());
        verify(defaultIndexStore, never()).onFinishParse(any(ByteBuf.class));
        
        // Step 3: GTID + EXEC command
        ByteBuf execBuf = createRedisArrayCommand("GTID", gtid, "0", "EXEC");
        Object[] execPayload = createCommandPayload("GTID", gtid, "0", "EXEC");
        streamCommandReader.onCommand(execPayload, execBuf);
        
        // Verify transaction was committed
        Assert.assertFalse("Transaction should not be active", streamCommandReader.isTransactionActive());
        Assert.assertEquals(0, streamCommandReader.getTransactionSize());
        
        // Verify onCommand was called with GTID
        verify(defaultIndexStore, times(1)).onCommand(eq(gtid), anyLong());
        
        // Verify all commands were written (MULTI + SET + GTID+EXEC = 3)
        verify(defaultIndexStore, times(3)).onFinishParse(any(ByteBuf.class));
        
        // Verify ByteBufs were properly handled
        Assert.assertEquals("MULTI ByteBuf refCnt should remain unchanged", 1, multiBuf.refCnt());
        Assert.assertEquals("SET ByteBuf refCnt should remain unchanged", 1, setBuf.refCnt());
        Assert.assertEquals("EXEC ByteBuf refCnt should remain unchanged", 1, execBuf.refCnt());
    }

    @Test
    public void testTransactionByteBufRelease() throws IOException {
        String gtid = "a4f566ef50a85e1119f17f9b746728b48609a2ab:3";
        
        // Create ByteBufs for transaction
        ByteBuf multiBuf = createRedisArrayCommand("MULTI");
        ByteBuf setBuf = createRedisArrayCommand("SET", "key1", "value1");
        ByteBuf execBuf = createRedisArrayCommand("GTID", gtid, "0", "EXEC");
        
        // Retain to simulate external ownership
        multiBuf.retain();
        setBuf.retain();
        execBuf.retain();
        
        int multiRefCnt = multiBuf.refCnt();
        int setRefCnt = setBuf.refCnt();
        int execRefCnt = execBuf.refCnt();
        
        // Process MULTI
        Object[] multiPayload = createCommandPayload("MULTI");
        streamCommandReader.onCommand(multiPayload, multiBuf);
        
        // Process SET in transaction
        Object[] setPayload = createCommandPayload("SET", "key1", "value1");
        streamCommandReader.onCommand(setPayload, setBuf);
        
        // Process EXEC
        Object[] execPayload = createCommandPayload("GTID", gtid, "0", "EXEC");
        streamCommandReader.onCommand(execPayload, execBuf);
        
        // Verify transaction completed
        Assert.assertFalse("Transaction should not be active", streamCommandReader.isTransactionActive());
        
        // Release the retained references (simulating external release)
        multiBuf.release();
        setBuf.release();
        execBuf.release();
        
        // Verify ByteBufs are still valid (reader should have retained them internally)
        Assert.assertTrue("MULTI ByteBuf should still be valid", multiBuf.refCnt() > 0);
        Assert.assertTrue("SET ByteBuf should still be valid", setBuf.refCnt() > 0);
        Assert.assertTrue("EXEC ByteBuf should still be valid", execBuf.refCnt() > 0);
        
        // Clean up - release the internal references
        multiBuf.release();
        setBuf.release();
        execBuf.release();
    }

    @Test
    public void testNestedMulti() throws IOException {
        // First MULTI
        ByteBuf multi1Buf = createRedisArrayCommand("MULTI");
        Object[] multi1Payload = createCommandPayload("MULTI");
        streamCommandReader.onCommand(multi1Payload, multi1Buf);
        
        Assert.assertTrue("First transaction should be active", streamCommandReader.isTransactionActive());
        
        // Second MULTI (should clear first transaction)
        ByteBuf multi2Buf = createRedisArrayCommand("MULTI");
        Object[] multi2Payload = createCommandPayload("MULTI");
        streamCommandReader.onCommand(multi2Payload, multi2Buf);
        
        // Should only have one command (the second MULTI)
        Assert.assertTrue("Second transaction should be active", streamCommandReader.isTransactionActive());
        Assert.assertEquals(1, streamCommandReader.getTransactionSize());
    }

    @Test
    public void testExecWithoutMulti() throws IOException {
        String gtid = "a4f566ef50a85e1119f17f9b746728b48609a2ab:4";
        
        // EXEC without MULTI (should be treated as regular GTID command)
        ByteBuf execBuf = createRedisArrayCommand("GTID", gtid, "0", "EXEC");
        Object[] execPayload = createCommandPayload("GTID", gtid, "0", "EXEC");
        
        streamCommandReader.onCommand(execPayload, execBuf);
        
        // Should be processed as regular GTID command
        Assert.assertFalse("Transaction should not be active", streamCommandReader.isTransactionActive());
        verify(defaultIndexStore, times(1)).onCommand(eq(gtid), anyLong());
        verify(defaultIndexStore, times(1)).onFinishParse(any(ByteBuf.class));
    }

    @Test
    public void testMultipleRegularCommands() throws IOException {
        // Process multiple regular commands
        for (int i = 0; i < 5; i++) {
            ByteBuf commandBuf = createRedisArrayCommand("SET", "key" + i, "value" + i);
            Object[] payload = createCommandPayload("SET", "key" + i, "value" + i);
            streamCommandReader.onCommand(payload, commandBuf);
            Assert.assertEquals(1, commandBuf.refCnt());
        }
        
        // Verify all commands were processed
        verify(defaultIndexStore, times(5)).onFinishParse(any(ByteBuf.class));
        verify(defaultIndexStore, never()).onCommand(anyString(), anyLong());
    }

    @Test
    public void testResetParser() throws IOException {
        String gtid = "a4f566ef50a85e1119f17f9b746728b48609a2ab:5";
        
        // Start a transaction
        ByteBuf multiBuf = createRedisArrayCommand("MULTI");
        Object[] multiPayload = createCommandPayload("MULTI");
        streamCommandReader.onCommand(multiPayload, multiBuf);
        
        Assert.assertTrue("Transaction should be active", streamCommandReader.isTransactionActive());
        
        // Reset parser
        streamCommandReader.resetParser();
        
        // Transaction should be cleared
        Assert.assertFalse("Transaction should not be active after reset", streamCommandReader.isTransactionActive());
        Assert.assertEquals(0, streamCommandReader.getTransactionSize());
    }

    // Helper methods to create Redis protocol commands

    private ByteBuf createRedisArrayCommand(String... args) {
        ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer();
        buffer.writeByte(RedisClientProtocol.ASTERISK_BYTE);
        buffer.writeBytes(String.valueOf(args.length).getBytes());
        buffer.writeBytes("\r\n".getBytes());
        for (String arg : args) {
            ByteBuf bulkString = createBulkString(arg);
            try {
                buffer.writeBytes(bulkString);
            } finally {
                // Release the bulk string buffer after copying
                if (bulkString.refCnt() > 0) {
                    bulkString.release();
                }
            }
        }
        return buffer;
    }

    private ByteBuf createBulkString(String str) {
        ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer();
        buffer.writeByte(RedisClientProtocol.DOLLAR_BYTE);
        buffer.writeBytes(String.valueOf(str.length()).getBytes());
        buffer.writeBytes("\r\n".getBytes());
        buffer.writeBytes(str.getBytes());
        buffer.writeBytes("\r\n".getBytes());
        return buffer;
    }

    private Object[] createCommandPayload(String... args) {
        Object[] payload = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            ByteArrayOutputStreamPayload byteArrayPayload = new ByteArrayOutputStreamPayload();
            try {
                byteArrayPayload.startInput();
                byteArrayPayload.in(Unpooled.wrappedBuffer(args[i].getBytes()));
                byteArrayPayload.endInput();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            payload[i] = byteArrayPayload;
        }
        return payload;
    }
}

