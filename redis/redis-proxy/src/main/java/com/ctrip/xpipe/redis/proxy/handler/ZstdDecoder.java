package com.ctrip.xpipe.redis.proxy.handler;

import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.DecompressionException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.Adler32;

import static com.ctrip.xpipe.redis.proxy.handler.ZstdConstants.*;

public class ZstdDecoder extends ByteToMessageDecoder {

    /**
     * Current state of stream.
     */
    private enum State {
        INIT_BLOCK,
        DECOMPRESS_DATA,
        FINISHED,
        CORRUPTED
    }

    private State currentState = State.INIT_BLOCK;

    private final Adler32 checksum = new Adler32();

    private int blockType;

    private int compressedLength;

    private int decompressedLength;

    private int currentChecksum;

    private boolean validateCheckSum;

    public ZstdDecoder() {
        this(true);
    }

    public ZstdDecoder(boolean validateCheckSum) {
        this.validateCheckSum = validateCheckSum;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            while(in.isReadable()) {
                switch (currentState) {
                    case INIT_BLOCK:
                        if (!isHeaderComplete(in)) {
                            return;
                        }

                        checkMagic(in);
                        analyzeHeader(in);
                        break;
                        // fall through
                    case DECOMPRESS_DATA:

                        if (in.readableBytes() < compressedLength) {
                            return;
                        }

                        ByteBuf uncompressed = null;

                        try {
                            switch (blockType) {
                                case BLOCK_TYPE_NON_COMPRESSED:
                                    // Just pass through, we not update the readerIndex yet as we do this outside of the
                                    // switch statement.
                                    uncompressed = in.retainedSlice(in.readerIndex(), decompressedLength);
                                    break;
                                case BLOCK_TYPE_COMPRESSED:
                                    uncompressed = ctx.alloc().buffer(decompressedLength, decompressedLength);

                                    Zstd.decompress(
                                            uncompressed.internalNioBuffer(uncompressed.writerIndex(), decompressedLength),
                                            safeNioBuffer(in));
                                    // Update the writerIndex now to reflect what we decompressed.
                                    uncompressed.writerIndex(uncompressed.writerIndex() + decompressedLength);
                                    break;
                                default:
                                    throw new DecompressionException(String.format(
                                            "unexpected blockType: %d (expected: %d or %d)",
                                            blockType, BLOCK_TYPE_NON_COMPRESSED, BLOCK_TYPE_COMPRESSED));
                            }
                            // Skip inbound bytes after we processed them.
                            in.skipBytes(compressedLength);

                            checkDecompressLength(uncompressed);
                            if (validateCheckSum) {
                                checkChecksum(uncompressed, currentChecksum);
                            }

                            out.add(uncompressed);
                            uncompressed = null;
                            currentState = State.INIT_BLOCK;
                        } catch (Exception e) {
                            throw new DecompressionException(e);
                        } finally {
                            if (uncompressed != null) {
                                uncompressed.release();
                            }
                        }
                        break;
                    case FINISHED:
                    case CORRUPTED:
                        in.skipBytes(in.readableBytes());
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }
        } catch (Exception e) {
            currentState = State.CORRUPTED;
            throw e;
        }
    }

    public boolean isClosed() {
        return currentState == State.FINISHED;
    }

    private void checkChecksum(ByteBuf uncompressed, int currentChecksum) {
        checksum.reset();
        checksum.update(uncompressed.nioBuffer(uncompressed.readerIndex(), uncompressed.readableBytes()));

        final int checksumResult = (int) checksum.getValue();
        if (checksumResult != currentChecksum) {
            throw new DecompressionException(String.format(
                    "stream corrupted: mismatching checksum: %d (expected: %d)",
                    checksumResult, currentChecksum));
        }
    }

    private ByteBuffer safeNioBuffer(ByteBuf buffer) {
        return buffer.nioBufferCount() == 1 ? buffer.internalNioBuffer(buffer.readerIndex(), this.compressedLength)
                : buffer.nioBuffer(buffer.readerIndex(), this.compressedLength);
    }

    private boolean isHeaderComplete(ByteBuf in) {
        return in.readableBytes() >= HEADER_LENGTH;
    }

    private void checkMagic(ByteBuf in) {
        final int magic = in.readInt();
        if (magic != MAGIC_NUMBER) {
            throw new DecompressionException("unexpected block identifier");
        }
    }

    private void analyzeHeader(ByteBuf in) {
        final int token = in.readByte();
        final int compressionLevel = (token & 0x0F) + COMPRESSION_LEVEL_BASE;
        int blockType = token & 0xF0;

        int compressedLength = Integer.reverseBytes(in.readInt());
        if (compressedLength < 0 || compressedLength > MAX_BLOCK_SIZE) {
            throw new DecompressionException(String.format(
                    "invalid compressedLength: %d (expected: 0-%d)",
                    compressedLength, MAX_BLOCK_SIZE));
        }

        int decompressedLength = Integer.reverseBytes(in.readInt());
        final int maxDecompressedLength = 1 << compressionLevel;
        if (decompressedLength < 0 || decompressedLength > maxDecompressedLength) {
            throw new DecompressionException(String.format(
                    "invalid decompressedLength: %d (expected: 0-%d)",
                    decompressedLength, maxDecompressedLength));
        }
        if (decompressedLength == 0 && compressedLength != 0) {
            throw new DecompressionException(String.format(
                    "stream corrupted: compressedLength(%d) and decompressedLength(%d) mismatch",
                    compressedLength, decompressedLength));
        }
        if (decompressedLength != 0 && compressedLength == 0
                || blockType == BLOCK_TYPE_NON_COMPRESSED && decompressedLength != compressedLength) {
            throw new DecompressionException(String.format(
                    "stream corrupted: compressedLength(%d) and decompressedLength(%d) mismatch",
                    compressedLength, decompressedLength));
        }

        int currentChecksum = Integer.reverseBytes(in.readInt());
        if (decompressedLength == 0 && compressedLength == 0) {
            if (currentChecksum != 0) {
                throw new DecompressionException("stream corrupted: checksum error");
            }
            currentState = State.FINISHED;
            return;
        }

        this.blockType = blockType;
        this.compressedLength = compressedLength;
        this.decompressedLength = decompressedLength;
        this.currentChecksum = currentChecksum;

        currentState = State.DECOMPRESS_DATA;
    }

    private void checkDecompressLength(ByteBuf uncompressed) {
        if(this.decompressedLength != uncompressed.readableBytes()) {
            throw new DecompressionException(String.format(
                    "stream decode length error: mismatching decompress length: %d (expected: %d)",
                    uncompressed.readableBytes(), decompressedLength));
        }
    }
}
