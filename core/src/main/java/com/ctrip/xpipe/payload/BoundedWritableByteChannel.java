package com.ctrip.xpipe.payload;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.function.Consumer;

/**
 * A WritableByteChannel implementation that limits memory usage by using a fixed-size buffer.
 * When the buffer is full, it triggers processing of the buffered data.
 * 
 * @author x-pipe
 */
public class BoundedWritableByteChannel implements WritableByteChannel {
    
    private static final Logger logger = LoggerFactory.getLogger(BoundedWritableByteChannel.class);
    
    private final int bufferSize;
    private final ByteBuffer buffer;
    private final Consumer<ByteBuf> processor;
    private boolean closed = false;
    
    /**
     * Creates a new BoundedWritableByteChannel with the specified buffer size.
     * 
     * @param bufferSize the maximum size of the buffer in bytes
     * @param processor the callback to process buffered data when buffer is full or channel is closed
     */
    public BoundedWritableByteChannel(int bufferSize, Consumer<ByteBuf> processor) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be positive: " + bufferSize);
        }
        if (processor == null) {
            throw new IllegalArgumentException("Processor cannot be null");
        }
        this.bufferSize = bufferSize;
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.processor = processor;
    }
    
    @Override
    public boolean isOpen() {
        return !closed;
    }
    
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        
        // Process any remaining data in the buffer
        flush();
    }
    
    @Override
    public int write(ByteBuffer src) throws IOException {
        if (closed) {
            throw new IOException("Channel is closed");
        }
        
        int totalWritten = 0;
        int remaining = src.remaining();
        
        while (remaining > 0) {
            // If buffer is full, flush it first
            if (buffer.remaining() == 0) {
                flush();
            }
            
            // Copy data from src to buffer
            int toCopy = Math.min(buffer.remaining(), remaining);
            int oldLimit = src.limit();
            src.limit(src.position() + toCopy);
            buffer.put(src);
            src.limit(oldLimit);
            
            totalWritten += toCopy;
            remaining -= toCopy;
            
            // If buffer is full after this write, flush it
            if (buffer.remaining() == 0) {
                flush();
            }
        }
        
        return totalWritten;
    }
    
    /**
     * Flushes the current buffer content to the processor.
     * This method is called automatically when the buffer is full or when the channel is closed.
     */
    public void flush() throws IOException {
        if (buffer.position() == 0) {
            return; // Nothing to flush
        }
        
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        buffer.clear();
        
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
        try {
            processor.accept(byteBuf);
        } catch (Exception e) {
            logger.error("[flush] Error processing buffered data", e);
            throw new IOException("Error processing buffered data", e);
        } finally {
            byteBuf.release();
        }
    }
    
    /**
     * Gets the current buffer size limit.
     */
    public int getBufferSize() {
        return bufferSize;
    }
    
    /**
     * Gets the number of bytes currently in the buffer.
     */
    public int getCurrentBufferSize() {
        return buffer.position();
    }
}

