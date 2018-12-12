package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.utils.VisibleForTesting;
import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.compression.CompressionException;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.ObjectUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;

import static com.ctrip.xpipe.redis.proxy.handler.ZstdConstants.*;
import static io.netty.util.internal.ThrowableUtil.unknownStackTrace;


/**
 *
 *  * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *     * * * * * * * * * * * * *
 *  * Magic * CompressType *  Compressed *  Decompressed *  Checksum *  +  *  ZSTD compressed *
 *  *       *              *    length   *     length    *           *     *      block      *
 *  * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *     * * * * * * * * * * * * *
 */

public class ZstdEncoder extends MessageToByteEncoder<ByteBuf> {


    private static final EncoderException ENCODE_FINSHED_EXCEPTION = unknownStackTrace(new EncoderException(
                    new IllegalStateException("encode finished and not enough space to write remaining data")),
            ZstdEncoder.class, "encode");

    private final int blockSize;

    private final Adler32 checksum = new Adler32();

    private final int compressionLevel;

    private ByteBuf buffer;

    private final int maxEncodeSize;

    private volatile boolean finished;

    private volatile ChannelHandlerContext ctx;


    public ZstdEncoder() {
        this(DEFAULT_BLOCK_SIZE, MAX_BLOCK_SIZE);
    }


    public ZstdEncoder(int blockSize, int maxEncodeSize) {
        super(true);
        compressionLevel = compressionLevel(blockSize);
        this.blockSize = blockSize;
        this.maxEncodeSize = ObjectUtil.checkPositive(maxEncodeSize, "maxEncodeSize");
        finished = false;
    }


    @VisibleForTesting
    protected static int compressionLevel(int blockSize) {
        if (blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "blockSize: %d (expected: %d-%d)", blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE));
        }
        int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
        compressionLevel = Math.max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
        return compressionLevel;
    }

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, ByteBuf msg, boolean preferDirect) {
        return allocateBuffer(ctx, msg, preferDirect, true);
    }

    private ByteBuf allocateBuffer(ChannelHandlerContext ctx, ByteBuf msg, boolean preferDirect,
                                   boolean allowEmptyReturn) {
        int targetBufSize = 0;
        int remaining = msg.readableBytes() + buffer.readableBytes();

        // quick overflow check
        if (remaining < 0) {
            throw new EncoderException("too much data to allocate a buffer for compression");
        }

        while (remaining > 0) {
            int curSize = Math.min(blockSize, remaining);
            remaining -= curSize;
            if(curSize <= MIN_BLOCK_SIZE) {
                targetBufSize += curSize + HEADER_LENGTH;
            } else {
                targetBufSize += Zstd.compressBound(curSize) + HEADER_LENGTH;
            }
        }

        if (targetBufSize > maxEncodeSize || 0 > targetBufSize) {
            throw new EncoderException(String.format("requested encode buffer size (%d bytes) exceeds the maximum " +
                    "allowable size (%d bytes)", targetBufSize, maxEncodeSize));
        }

        if (preferDirect) {
            return ctx.alloc().ioBuffer(targetBufSize, targetBufSize);
        } else {
            return ctx.alloc().heapBuffer(targetBufSize, targetBufSize);
        }
    }


    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf in, ByteBuf out) throws Exception {
        if (finished) {
            if (!out.isWritable(in.readableBytes())) {
                // out should be EMPTY_BUFFER because we should have allocated enough space above in allocateBuffer.
                throw ENCODE_FINSHED_EXCEPTION;
            }
            out.writeBytes(in);
            return;
        }

        final ByteBuf buffer = this.buffer;
        int length;
        while ((length = in.readableBytes()) > 0) {
            final int nextChunkSize = Math.min(length, buffer.writableBytes());
            in.readBytes(buffer, nextChunkSize);

            if(nextChunkSize <= MIN_BLOCK_SIZE) {
                writeUnCompressedData(out);
                return;
            }

            if (!buffer.isWritable()) {
                flushBufferedData(out);
            }
        }
        if(buffer.isReadable()) {
            flushBufferedData(out);
        }
    }

    private void flushBufferedData(ByteBuf out) {
        final int flushableBytes = buffer.readableBytes();
        if (flushableBytes == 0) {
            return;
        }
        checksum.reset();
        checksum.update(buffer.internalNioBuffer(buffer.readerIndex(), flushableBytes));
        final int check = (int) checksum.getValue();

        final int bufSize = (int) Zstd.compressBound(flushableBytes) + HEADER_LENGTH;
        out.ensureWritable(bufSize);
        final int idx = out.writerIndex();
        int compressedLength;
        try {
            ByteBuffer outNioBuffer = out.internalNioBuffer(idx + HEADER_LENGTH, out.writableBytes() - HEADER_LENGTH);
            compressedLength = Zstd.compress(
                    outNioBuffer,
                    buffer.internalNioBuffer(buffer.readerIndex(), flushableBytes),
                    DEFAULT_COMPRESS_LEVEL);
        } catch (Exception e) {
            throw new CompressionException(e);
        }
        final int blockType;
        if (compressedLength >= flushableBytes) {
            blockType = BLOCK_TYPE_NON_COMPRESSED;
            compressedLength = flushableBytes;
            out.setBytes(idx + HEADER_LENGTH, buffer, 0, flushableBytes);
        } else {
            blockType = BLOCK_TYPE_COMPRESSED;
        }

        out.setInt(idx, MAGIC_NUMBER);
        out.setByte(idx + TOKEN_OFFSET, (byte) (blockType | compressionLevel));
        out.setIntLE(idx + COMPRESSED_LENGTH_OFFSET, compressedLength);
        out.setIntLE(idx + DECOMPRESSED_LENGTH_OFFSET, flushableBytes);
        out.setIntLE(idx + CHECKSUM_OFFSET, check);
        out.writerIndex(idx + HEADER_LENGTH + compressedLength);
        buffer.clear();
    }

    private void writeUnCompressedData(ByteBuf out) {
        int flushableBytes = buffer.readableBytes();
        if (flushableBytes == 0) {
            return;
        }
        checksum.reset();
        checksum.update(buffer.internalNioBuffer(buffer.readerIndex(), flushableBytes));
        final int check = (int) checksum.getValue();

        out.ensureWritable(flushableBytes + HEADER_LENGTH);
        final int idx = out.writerIndex();
        out.setBytes(idx + HEADER_LENGTH, buffer, 0, flushableBytes);

        out.setInt(idx, MAGIC_NUMBER);
        out.setByte(idx + TOKEN_OFFSET, (byte) BLOCK_TYPE_NON_COMPRESSED);
        out.setIntLE(idx + COMPRESSED_LENGTH_OFFSET, flushableBytes);
        out.setIntLE(idx + DECOMPRESSED_LENGTH_OFFSET, flushableBytes);
        out.setIntLE(idx + CHECKSUM_OFFSET, check);
        out.writerIndex(idx + HEADER_LENGTH + flushableBytes);
        buffer.clear();
    }

    @Override
    public void flush(final ChannelHandlerContext ctx) throws Exception {
        if (buffer != null && buffer.isReadable()) {
            final ByteBuf buf = allocateBuffer(ctx, Unpooled.EMPTY_BUFFER, isPreferDirect(), false);
            flushBufferedData(buf);
            ctx.write(buf);
        }
        ctx.flush();
    }

    private ChannelFuture finishEncode(final ChannelHandlerContext ctx, ChannelPromise promise) {
        if (finished) {
            promise.setSuccess();
            return promise;
        }
        finished = true;

        final ByteBuf footer = ctx.alloc().ioBuffer(
                (int) Zstd.compressBound(buffer.readableBytes()) + HEADER_LENGTH);
        flushBufferedData(footer);

        final int idx = footer.writerIndex();
        footer.setInt(idx, MAGIC_NUMBER);
        footer.setByte(idx + TOKEN_OFFSET, (byte) (BLOCK_TYPE_NON_COMPRESSED | compressionLevel));
        footer.setInt(idx + COMPRESSED_LENGTH_OFFSET, 0);
        footer.setInt(idx + DECOMPRESSED_LENGTH_OFFSET, 0);
        footer.setInt(idx + CHECKSUM_OFFSET, 0);

        footer.writerIndex(idx + HEADER_LENGTH);

        return ctx.writeAndFlush(footer, promise);
    }


    public boolean isClosed() {
        return finished;
    }


    public ChannelFuture close() {
        return close(ctx().newPromise());
    }


    public ChannelFuture close(final ChannelPromise promise) {
        ChannelHandlerContext ctx = ctx();
        EventExecutor executor = ctx.executor();
        if (executor.inEventLoop()) {
            return finishEncode(ctx, promise);
        } else {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    ChannelFuture f = finishEncode(ctx(), promise);
                    f.addListener(new ChannelPromiseNotifier(promise));
                }
            });
            return promise;
        }
    }

    @Override
    public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception {
        ChannelFuture f = finishEncode(ctx, ctx.newPromise());
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                ctx.close(promise);
            }
        });

        if (!f.isDone()) {
            // Ensure the channel is closed even if the write operation completes in time.
            ctx.executor().schedule(new Runnable() {
                @Override
                public void run() {
                    ctx.close(promise);
                }
            }, 10, TimeUnit.SECONDS);
        }
    }

    private ChannelHandlerContext ctx() {
        ChannelHandlerContext ctx = this.ctx;
        if (ctx == null) {
            throw new IllegalStateException("not added to a pipeline");
        }
        return ctx;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        buffer = ctx.alloc().ioBuffer(blockSize);
        buffer.clear();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        if (buffer != null) {
            buffer.release();
            buffer = null;
        }
    }

}
