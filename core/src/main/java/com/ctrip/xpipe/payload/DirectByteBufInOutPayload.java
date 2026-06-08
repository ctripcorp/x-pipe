package com.ctrip.xpipe.payload;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

/**
 * Stream replication path payload: accumulates bulk string data in direct memory via CompositeByteBuf.
 * Off-heap memory is released by external {@link #reset()}, not in {@link #doEndInput()}.
 */
public class DirectByteBufInOutPayload extends AbstractInOutPayload {

    private static final Logger logger = LoggerFactory.getLogger(DirectByteBufInOutPayload.class);

    private static final int INIT_SIZE = 1 << 8;

    private ByteBuf cumulation;

    @Override
    protected void doStartInput() {
        super.doStartInput();
        cumulation = Unpooled.compositeBuffer(INIT_SIZE);
    }

    @Override
    protected int doIn(ByteBuf byteBuf) throws IOException {
        cumulation = DirectByteBufInStringOutPayload.COMPOSITE_CUMULATOR.cumulate(cumulation, byteBuf.retain());
        return byteBuf.readableBytes();
    }

    @Override
    protected void doTruncate(int reduceLen) throws IOException {
        cumulation.writerIndex(cumulation.writerIndex() - reduceLen);
    }

    @Override
    protected long doOut(WritableByteChannel writableByteChannel) throws IOException {
        int readable = cumulation.readableBytes();
        ByteBuffer[] buffers = cumulation.nioBuffers();
        int writeOffset = 0;
        if (buffers != null) {
            for (ByteBuffer buffer : buffers) {
                writeOffset += writableByteChannel.write(buffer);
            }
        }
        if (writeOffset < readable) {
            logger.warn("[doOut][wrote < readable]{} < {}", writeOffset, readable);
        }
        return writeOffset;
    }

    @Override
    protected void doEndInput() throws IOException {
        // Off-heap memory is released by external reset()
    }

    @Override
    public String toString() {
        return cumulation.toString(Charset.defaultCharset());
    }

    /**
     * Copy readable bytes without advancing readerIndex.
     */
    public byte[] getBytes() {
        int len = cumulation.readableBytes();
        byte[] bytes = new byte[len];
        cumulation.getBytes(cumulation.readerIndex(), bytes);
        return bytes;
    }

    public void reset() {
        try {
            if (cumulation != null) {
                cumulation.release();
                cumulation = null;
            }
        } catch (Throwable t) {
            // ignore
        }
    }

    public boolean equalsIgnoreCaseAsciiExpectedUppercase(byte[] expectedUpperCaseAscii) {
        if (expectedUpperCaseAscii == null || cumulation == null) {
            return false;
        }
        int currentPos = cumulation.readableBytes();
        if (currentPos != expectedUpperCaseAscii.length) {
            return false;
        }
        for (int i = 0; i < currentPos; i++) {
            byte value = cumulation.getByte(cumulation.readerIndex() + i);
            if (value >= 'a' && value <= 'z') {
                value = (byte) (value - ('a' - 'A'));
            }
            if (value != expectedUpperCaseAscii[i]) {
                return false;
            }
        }
        return true;
    }
}
