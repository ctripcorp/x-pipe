package com.ctrip.xpipe.redis.proxy.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class ZstdEncoder extends MessageToByteEncoder<ByteBuf> {

    private static final int LEVEL_AUTO = 0;

    private static final int LEVEL_1 = 1;

    private static final int LEVEL_2 = 2;

    private static final int MAX_CHUNK_LENGTH = 0xFFFF;

    private static final int MAX_DISTANCE = 8191;
    private static final int MAX_FARDISTANCE = 65535 + MAX_DISTANCE - 1;

    private static final int HASH_LOG = 13;
    private static final int HASH_SIZE = 1 << HASH_LOG; // 8192
    private static final int HASH_MASK = HASH_SIZE - 1;

    private static final int MAX_COPY = 32;
    private static final int MAX_LEN = 256 + 8;

    private static final int MIN_RECOMENDED_LENGTH_FOR_LEVEL_2 = 1024 * 64;

    private static final int MAGIC_NUMBER = 'Z' << 24 | 'S' << 16 | 'T' << 8 | 'D';

    private static final byte BLOCK_TYPE_NON_COMPRESSED = 0x00;
    private static final byte     BLOCK_TYPE_COMPRESSED = 0x01;
    private static final byte    BLOCK_WITHOUT_CHECKSUM = 0x00;
    private static final byte       BLOCK_WITH_CHECKSUM = 0x10;

    private static final int OPTIONS_OFFSET = 3;
    private static final int CHECKSUM_OFFSET = 4;

    private final int level;

    private final Checksum checksum;

    private static final int MIN_LENGTH_TO_COMPRESSION = 32;


    public ZstdEncoder() {
        this(LEVEL_AUTO, null);
    }


    public ZstdEncoder(int level) {
        this(level, null);
    }


    public ZstdEncoder(boolean validateChecksums) {
        this(LEVEL_AUTO, validateChecksums ? new Adler32() : null);
    }

    /**
     * Creates a FastLZ encoder with specified compression level and checksum calculator.
     *
     * @param level supports only these values:
     *        0 - Encoder will choose level automatically depending on the length of the input buffer.
     *        1 - Level 1 is the fastest compression and generally useful for short data.
     *        2 - Level 2 is slightly slower but it gives better compression ratio.
     * @param checksum
     *        the {@link Checksum} instance to use to check data for integrity.
     *        You may set {@code null} if you don't want to validate checksum of each block.
     */
    public ZstdEncoder(int level, Checksum checksum) {
        super(false);
        if (level != LEVEL_AUTO && level != LEVEL_1 && level != LEVEL_2) {
            throw new IllegalArgumentException(String.format(
                    "level: %d (expected: %d or %d or %d)", level, LEVEL_AUTO, LEVEL_1, LEVEL_2));
        }
        this.level = level;
        this.checksum = checksum;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf in, ByteBuf out) throws Exception {
        final Checksum checksum = this.checksum;

        for (;;) {
            if (!in.isReadable()) {
                return;
            }
            final int idx = in.readerIndex();
            final int length = Math.min(in.readableBytes(), MAX_CHUNK_LENGTH);

            final int outputIdx = out.writerIndex();
            out.setMedium(outputIdx, MAGIC_NUMBER);
            int outputOffset = outputIdx + CHECKSUM_OFFSET + (checksum != null ? 4 : 0);

            final byte blockType;
            final int chunkLength;
            if (length < MIN_LENGTH_TO_COMPRESSION) {
                blockType = BLOCK_TYPE_NON_COMPRESSED;

                out.ensureWritable(outputOffset + 2 + length);
                final byte[] output = out.array();
                final int outputPtr = out.arrayOffset() + outputOffset + 2;

                if (checksum != null) {
                    final byte[] input;
                    final int inputPtr;
                    if (in.hasArray()) {
                        input = in.array();
                        inputPtr = in.arrayOffset() + idx;
                    } else {
                        input = new byte[length];
                        in.getBytes(idx, input);
                        inputPtr = 0;
                    }

                    checksum.reset();
                    checksum.update(input, inputPtr, length);
                    out.setInt(outputIdx + CHECKSUM_OFFSET, (int) checksum.getValue());

                    System.arraycopy(input, inputPtr, output, outputPtr, length);
                } else {
                    in.getBytes(idx, output, outputPtr, length);
                }
                chunkLength = length;
            } else {
                // try to compress
                final byte[] input;
                final int inputPtr;
                if (in.hasArray()) {
                    input = in.array();
                    inputPtr = in.arrayOffset() + idx;
                } else {
                    input = new byte[length];
                    in.getBytes(idx, input);
                    inputPtr = 0;
                }

                if (checksum != null) {
                    checksum.reset();
                    checksum.update(input, inputPtr, length);
                    out.setInt(outputIdx + CHECKSUM_OFFSET, (int) checksum.getValue());
                }

//                final int maxOutputLength = calculateOutputBufferLength(length);
                out.ensureWritable(outputOffset + 4 + maxOutputLength);
                final byte[] output = out.array();
                final int outputPtr = out.arrayOffset() + outputOffset + 4;
//                final int compressedLength = compress(input, inputPtr, length, output, outputPtr, level);
                if (compressedLength < length) {
                    blockType = BLOCK_TYPE_COMPRESSED;
                    chunkLength = compressedLength;

                    out.setShort(outputOffset, chunkLength);
                    outputOffset += 2;
                } else {
                    blockType = BLOCK_TYPE_NON_COMPRESSED;
                    System.arraycopy(input, inputPtr, output, outputPtr - 2, length);
                    chunkLength = length;
                }
            }
            out.setShort(outputOffset, length);

            out.setByte(outputIdx + OPTIONS_OFFSET,
                    blockType | (checksum != null ? BLOCK_WITH_CHECKSUM : BLOCK_WITHOUT_CHECKSUM));
            out.writerIndex(outputOffset + 2 + chunkLength);
            in.skipBytes(length);
        }
    }
}
