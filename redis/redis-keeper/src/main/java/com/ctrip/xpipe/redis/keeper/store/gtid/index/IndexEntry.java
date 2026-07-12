package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class IndexEntry {

    // v2
    private IndexEntryType type;
    private byte flags;
    private long cmdEndOffset;           // v2 GTID 条目结束在 cmd 文件的绝对位置（写完后 offset+cmdLength）

    private String uuid;
    private long startGno;
    private long cmdStartOffset;
    private long blockStartOffset;
    private long blockEndOffset;
    private int size;
    private long extReserved;
    private long position;


    private static final String SEPARATOR = RedisProtocol.CRLF;

    public static final int SEGMENT_LENGTH = RedisProtocol.RUN_ID_LENGTH + Long.BYTES * 4 + SEPARATOR.getBytes().length + Integer.BYTES;

    // v2
    public static final int SEGMENT_LENGTH_V2 = 96;
    public static final String ZONE_UUID;
    static {
        char[] chars = new char[RedisProtocol.RUN_ID_LENGTH];
        Arrays.fill(chars, '0');
        ZONE_UUID = new String(chars);
    }

    // 构造器（GTID 条目）
    public IndexEntry(String uuid, long startGno, long cmdStartOffset, long blockStartOffset) {
        this.uuid = uuid;
        this.startGno = startGno;
        this.cmdStartOffset = cmdStartOffset;
        this.blockStartOffset = blockStartOffset;
        this.blockEndOffset = -1;
        this.size = 0;
        this.type = IndexEntryType.GTID;
        this.flags = 0;
        this.cmdEndOffset = 0L;
        this.position = -1;
    }

    // ZONE 工厂方法
    public static IndexEntry zone(long zoneStart, long zoneEnd, int cmdCount) {
        IndexEntry e = new IndexEntry(ZONE_UUID, 0L, zoneStart, 0L);
        e.setBlockEndOffset(0L);
        e.setSize(cmdCount);
        e.type = IndexEntryType.ZONE;
        e.cmdEndOffset = zoneEnd;
        return e;
    }

    public boolean isZone() {
        return type == IndexEntryType.ZONE || ZONE_UUID.equals(uuid);
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public long getEndGno() {
        return startGno + size - 1;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public long getStartGno() {
        return startGno;
    }

    public void setStartGno(long startGno) {
        this.startGno = startGno;
    }

    public long getCmdStartOffset() {
        return cmdStartOffset;
    }

    public void setCmdStartOffset(long cmdStartOffset) {
        this.cmdStartOffset = cmdStartOffset;
    }

    public long getBlockStartOffset() {
        return blockStartOffset;
    }

    public void setBlockStartOffset(long blockStartOffset) {
        this.blockStartOffset = blockStartOffset;
    }

    public long getBlockEndOffset() {
        return blockEndOffset;
    }

    public void setBlockEndOffset(long blockEndOffset) {
        this.blockEndOffset = blockEndOffset;
    }

    public long getZoneStart() { return cmdStartOffset; }
    public long getZoneEnd() { return cmdEndOffset; }
    public long getCmdEndOffset() { return cmdEndOffset; }
    public void setCmdEndOffset(long cmdEndOffset) { this.cmdEndOffset = cmdEndOffset; }
    public IndexEntryType getType() { return type; }
    public byte getFlags() { return flags; }

    // ---------- v2 序列化（SEGMENT_LENGTH_V2 字节，大端）----------

    /** Async write path (spec §3.7.4): caller supplies a retained {@link ByteBuf} with {@link #SEGMENT_LENGTH_V2} writable bytes. */
    public void encodeV2(ByteBuf buf) {
        buf.writeByte(type.code());
        buf.writeByte(flags);
        buf.writeShort(0);
        buf.writeBytes(uuid.getBytes());
        buf.writeLong(startGno);
        buf.writeLong(cmdStartOffset);
        buf.writeLong(cmdEndOffset);
        buf.writeLong(blockStartOffset);
        buf.writeLong(blockEndOffset);
        buf.writeInt(size);
        buf.writeLong(extReserved);
    }

    /** V1 truncate-then-write path: caller supplies a {@link ByteBuf} with {@link #SEGMENT_LENGTH} writable bytes. */
    public void encode(ByteBuf buf) {
        buf.writeBytes(uuid.getBytes());
        buf.writeLong(startGno);
        buf.writeLong(cmdStartOffset);
        buf.writeLong(blockStartOffset);
        buf.writeLong(blockEndOffset);
        buf.writeInt(size);
        buf.writeBytes(SEPARATOR.getBytes());
    }

    public ByteBuffer generateBufferV2() {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(SEGMENT_LENGTH_V2);
        try {
            encodeV2(buf);
            byte[] bytes = new byte[SEGMENT_LENGTH_V2];
            buf.readBytes(bytes);
            return ByteBuffer.wrap(bytes);
        } finally {
            buf.release();
        }
    }

    public ByteBuffer generateBuffer() {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(SEGMENT_LENGTH);
        try {
            encode(buf);
            byte[] bytes = new byte[SEGMENT_LENGTH];
            buf.readBytes(bytes);
            return ByteBuffer.wrap(bytes);
        } finally {
            buf.release();
        }
    }

    /**
     * 从 Netty {@link ByteBuf} 读取 v2 格式条目；消费 {@link #SEGMENT_LENGTH_V2} 字节。
     */
    public static IndexEntry fromBufferV2(ByteBuf buffer) {
        if (buffer.readableBytes() < SEGMENT_LENGTH_V2) {
            return null;
        }
        IndexEntryType type = IndexEntryType.fromCode(buffer.readByte());
        byte flags = buffer.readByte();
        buffer.readShort();

        byte[] uuidBytes = new byte[RedisProtocol.RUN_ID_LENGTH];
        buffer.readBytes(uuidBytes);
        String uuid = new String(uuidBytes);

        long startGno = buffer.readLong();
        long cmdStartOffset = buffer.readLong();
        long cmdEndOffset = buffer.readLong();
        long blockStartOffset = buffer.readLong();
        long blockEndOffset = buffer.readLong();
        int size = buffer.readInt();
        long extReserved = buffer.readLong();

        IndexEntry e = new IndexEntry(uuid, startGno, cmdStartOffset, blockStartOffset);
        e.type = type;
        e.flags = flags;
        e.blockEndOffset = blockEndOffset;
        e.size = size;
        e.cmdEndOffset = cmdEndOffset;
        e.extReserved = extReserved;
        return e;
    }

    /** 从 {@link ByteBuffer} 读取 v2 格式条目（Reader 短读路径）。 */
    public static IndexEntry fromBufferV2(ByteBuffer buffer) {
        if (buffer.remaining() == 0 && buffer.position() > 0) {
            buffer.flip();
        }
        if (buffer.remaining() < SEGMENT_LENGTH_V2) {
            return null;
        }
        ByteBuf nettyBuf = ByteBufAllocator.DEFAULT.buffer(SEGMENT_LENGTH_V2);
        try {
            nettyBuf.writeBytes(buffer);
            return fromBufferV2(nettyBuf);
        } finally {
            nettyBuf.release();
        }
    }

    /**
     * 从 Netty {@link ByteBuf} 读取 v1 格式条目；消费 {@link #SEGMENT_LENGTH} 字节。
     */
    public static IndexEntry fromBuffer(ByteBuf buffer) {
        if (buffer.readableBytes() < SEGMENT_LENGTH) {
            return null;
        }
        byte[] uuidBytes = new byte[RedisProtocol.RUN_ID_LENGTH];
        buffer.readBytes(uuidBytes);
        String uuid = new String(uuidBytes);
        long startGno = buffer.readLong();
        long cmdStartOffset = buffer.readLong();
        long blockStartOffset = buffer.readLong();
        long blockEndOffset = buffer.readLong();
        int size = buffer.readInt();

        byte[] separatorBytes = new byte[SEPARATOR.getBytes().length];
        buffer.readBytes(separatorBytes);
        String separator = new String(separatorBytes);
        if (!SEPARATOR.equals(separator)) {
            throw new IllegalArgumentException("Invalid separator in buffer: " + separator);
        }
        IndexEntry indexEntry = new IndexEntry(uuid, startGno, cmdStartOffset, blockStartOffset);
        indexEntry.setBlockEndOffset(blockEndOffset);
        indexEntry.setSize(size);
        return indexEntry;
    }

    /** 从 {@link ByteBuffer} 读取 v1 格式条目（Reader 短读路径）。 */
    public static IndexEntry fromBuffer(ByteBuffer buffer) {
        buffer.flip();
        if (buffer.remaining() < SEGMENT_LENGTH) {
            return null;
        }
        ByteBuf nettyBuf = ByteBufAllocator.DEFAULT.buffer(buffer.remaining());
        try {
            nettyBuf.writeBytes(buffer);
            return fromBuffer(nettyBuf);
        } finally {
            nettyBuf.release();
        }
    }

    public void syncDataFromBlockEntry(BlockEntry blockEntry, AsyncFileSystem fs, AsyncFile blockFile)
            throws IOException {
        ByteBuf blockBuf = blockEntry.drainToByteBuf();
        if (blockBuf != null && blockBuf.isReadable()) {
            // AsyncFileSystem.write releases the buffer; caller must not release again.
            AsyncFileSystemHelper.writeAndAwait(fs, blockFile, blockBuf, blockBuf.readableBytes(),
                    "write block entry");
        }
        this.blockEndOffset = AsyncFileSystemHelper.await(fs.size(blockFile), "size block file");
        this.size = blockEntry.getSize();
    }

    public void saveToDiskV2(AsyncFileSystem fs, AsyncFile indexV2File, BlockEntry blockEntry, AsyncFile blockV2File)
            throws IOException {
        syncDataFromBlockEntry(blockEntry, fs, blockV2File);
        writeBufferV2(fs, indexV2File);
    }

    public void saveToDiskV2(AsyncFileSystem fs, AsyncFile indexV2File) throws IOException {
        writeBufferV2(fs, indexV2File);
    }

    private void writeBufferV2(AsyncFileSystem fs, AsyncFile indexV2File) throws IOException {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(SEGMENT_LENGTH_V2);
        try {
            encodeV2(buf);
            buf.retain();
            AsyncFileSystemHelper.writeAndAwait(fs, indexV2File, buf, SEGMENT_LENGTH_V2, "write index v2 entry");
        } finally {
            buf.release();
        }
    }

    /** V1 truncate-then-write semantics for async FS. */
    public void saveToDisk(AsyncFileSystem fs, AsyncFile indexFile, BlockEntry blockEntry, AsyncFile blockFile)
            throws IOException {
        syncDataFromBlockEntry(blockEntry, fs, blockFile);
        if (position > 0) {
            AsyncFileSystemHelper.await(fs.truncate(indexFile, position), "truncate index v1");
        } else {
            position = AsyncFileSystemHelper.await(fs.size(indexFile), "size index v1");
        }
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(SEGMENT_LENGTH);
        try {
            encode(buf);
            buf.retain();
            AsyncFileSystemHelper.writeAndAwait(fs, indexFile, buf, SEGMENT_LENGTH, "write index v1 entry");
        } finally {
            buf.release();
        }
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "uuid='" + uuid + '\'' +
                ", startGno=" + startGno +
                ", endGno=" + (startGno + size - 1) +
                ", cmdStartOffset=" + cmdStartOffset +
                ", cmdEndOffset=" + cmdEndOffset +
                ", blockStartOffset=" + blockStartOffset +
                ", blockEndOffset=" + blockEndOffset +
                ", size=" + size +
                ", position=" + position +
                '}';
    }

}
