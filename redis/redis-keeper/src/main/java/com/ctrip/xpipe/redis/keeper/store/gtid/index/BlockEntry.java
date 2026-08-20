package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * In-memory replacement for {@link BlockWriter} (spec §3.7.4).
 *
 * <p>Holds one GTID block's VarInt-encoded cmd-offset deltas plus its (uuid,startGno,currentGno,cmdOffset,size)
 * metadata. Owns no file/channel handle: IndexStore composes on-disk position via
 * {@code fs.size(blockFile) + BlockEntry#getPendingBytes()} and flushes bytes via
 * {@link #copyPending()} + {@code fs.write} + {@link #clearPending()} (write success then clear).
 * V1 and V2 index writers share this type.</p>
 *
 * <p>Uses a Netty {@link ByteBuf} internally so that (a) the buffer auto-grows past the pre-sized capacity
 * — {@code blockMaxSize} bounds gno count, but each VarInt delta is variable-length up to 5 bytes, so a
 * strict byte cap would risk overflow before {@link #isBlockFull()} fires — and (b) pending bytes can be
 * retained for {@code fs.write} without draining first (D7 / T-H3.CP-I.1).</p>
 */
public class BlockEntry implements AutoCloseable {

    public static final int DEFAULT_BLOCK_MAX_SIZE = 8 * 1024;

    /** VarInt worst case for a signed 32-bit delta. */
    private static final int MAX_VARINT_BYTES = 5;

    private final int blockMaxSize;
    private final ByteBufAllocator allocator;

    private String currentUuid;
    private long startGno;
    private long currentGno;
    private int cmdOffset;
    private int size;

    private ByteBuf blockCache;
    private boolean closed;

    public BlockEntry(String uuid, long gno, int cmdOffset) {
        this(uuid, gno, cmdOffset, DEFAULT_BLOCK_MAX_SIZE);
    }

    public BlockEntry(String uuid, long gno, int cmdOffset, int blockMaxSize) {
        this(uuid, gno, cmdOffset, blockMaxSize, ByteBufAllocator.DEFAULT);
    }

    public BlockEntry(String uuid, long gno, int cmdOffset, int blockMaxSize, ByteBufAllocator allocator) {
        this.blockMaxSize = blockMaxSize;
        this.allocator = allocator;
        this.blockCache = allocator.buffer(blockMaxSize * MAX_VARINT_BYTES);
        reset(uuid, gno, cmdOffset);
    }

    /**
     * Start a new logical block on a reused instance (e.g. {@code createNewBlock} / {@code changeBlock}).
     * Resets metadata and clears the internal cache; reuses buffer capacity when possible.
     * Caller must have finished with any prior {@link #drainToByteBuf()} output before reset.
     */
    public void reset(String uuid, long gno, int cmdOffset) {
        ensureOpen();
        this.currentUuid = uuid;
        this.startGno = gno;
        this.currentGno = gno;
        this.cmdOffset = cmdOffset;
        this.size = 0;
        int initialCapacity = blockMaxSize * MAX_VARINT_BYTES;
        if (blockCache.capacity() > initialCapacity) {
            blockCache.release();
            blockCache = allocator.buffer(initialCapacity);
        } else {
            blockCache.clear();
        }
    }

    public void append(String uuid, long gno, int offset) {
        ensureOpen();
        int delta = offset - cmdOffset;
        VarInt.encodeToByteBuf(delta, blockCache);
        this.cmdOffset = offset;
        this.currentGno = gno;
        this.currentUuid = uuid;
        this.size++;
    }

    public boolean isGnoGap(String uuid, long gno) {
        return !StringUtil.trimEquals(this.currentUuid, uuid) || gno != currentGno + 1;
    }

    /** Full is measured by gno count, not byte position — bytes auto-grow if a delta encodes wider than expected. */
    public boolean isBlockFull() {
        return size >= blockMaxSize;
    }

    public boolean needChangeBlock(String uuid, long gno) {
        return isBlockFull() || isGnoGap(uuid, gno);
    }

    /**
     * Independent copy of pending VarInt bytes for {@code writeAndAwait} (D7 / T-H3.CP-I.1).
     * Caller owns the returned buffer ({@code refCnt=1}); Helper/FS {@code release} it.
     * Do not {@link #drainToByteBuf()} or {@code retain} the live cache — TailCache may
     * keep {@code writeBuf=data} for async flush. After a successful write, {@link #clearPending()}.
     */
    public ByteBuf copyPending() {
        ensureOpen();
        return blockCache.copy();
    }

    /**
     * Drop pending VarInt bytes after a successful block write. Metadata (uuid/gno/cmdOffset/size)
     * is kept for incremental flushes.
     */
    public void clearPending() {
        ensureOpen();
        blockCache.clear();
    }

    /**
     * Hand the accumulated VarInt bytes to the caller — zero-copy ownership transfer; caller must
     * {@link ByteBuf#release()} after {@code fs.write(...)} completes. Internal buffer is swapped for a
     * fresh one. (uuid,gno,cmdOffset,size) metadata is preserved for subsequent incremental flushes.
     */
    public ByteBuf drainToByteBuf() {
        ensureOpen();
        ByteBuf out = blockCache;
        blockCache = allocator.buffer(blockMaxSize * MAX_VARINT_BYTES);
        return out;
    }

    /**
     * Rebuild in-memory state from persisted bytes on recover (spec §3.7.4).
     * The caller supplies the block's starting {@code (uuid, startGno, blockStartOffset)} — this method
     * walks the VarInt deltas to recompute {@code cmdOffset} and {@code size}. The internal cache is
     * reset to empty so future {@link #append} calls append fresh bytes without duplicating history.
     */
    public void recover(ByteBuf src, String uuid, long startGno, int blockStartOffset) {
        ensureOpen();
        this.currentUuid = uuid;
        this.startGno = startGno;
        this.currentGno = startGno;
        this.cmdOffset = blockStartOffset;
        this.size = 0;
        while (src.isReadable()) {
            int delta = VarInt.getVarInt(src);
            this.cmdOffset += delta;
            this.currentGno++;
            this.size++;
        }
        this.blockCache.clear();
    }

    public int getSize() {
        return size;
    }

    /** VarInt bytes in {@link #blockCache} not yet flushed (cleared only after a successful write). */
    public int getPendingBytes() {
        return blockCache.readableBytes();
    }

    public int getCmdOffset() {
        return cmdOffset;
    }

    public String getCurrentUuid() {
        return currentUuid;
    }

    public long getStartGno() {
        return startGno;
    }

    public long getCurrentGno() {
        return currentGno;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (blockCache != null) {
            blockCache.release();
            blockCache = null;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("BlockEntry is closed");
        }
    }
}
