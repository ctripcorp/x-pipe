package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.zip.CRC32C;

public class IndexEntry {

    // v2
    private IndexEntryType type;
    private byte flags;
    private int lastCommandLength;      // v2 最后一条命令的长度

    private String uuid;
    private long startGno;
    private long cmdStartOffset;
    private long blockStartOffset;
    private long blockEndOffset;
    private int size;
    private long position;


    private static final String SEPARATOR = RedisProtocol.CRLF;

    public static final int SEGMENT_LENGTH = RedisProtocol.RUN_ID_LENGTH + Long.BYTES * 4 + SEPARATOR.getBytes().length + Integer.BYTES;

    // v2
    public static final int SEGMENT_LENGTH_V2 = 88;
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
        this.lastCommandLength = 0;
        this.position = -1;
    }

    // ZONE 工厂方法
    public static IndexEntry zone(long zoneStart, long zoneEnd, int cmdCount) {
        IndexEntry e = new IndexEntry(ZONE_UUID, 0L, zoneStart, zoneEnd);
        e.setBlockEndOffset(zoneEnd);
        e.setSize(cmdCount);
        e.type = IndexEntryType.ZONE;
        e.lastCommandLength = 0;
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
    public long getZoneEnd() { return blockEndOffset; }
    public int getLastCommandLength() { return lastCommandLength; }
    public void setLastCommandLength(int length) { this.lastCommandLength = length; }
    public IndexEntryType getType() { return type; }
    public byte getFlags() { return flags; }

    // ---------- v2 序列化（88 字节，大端）----------
    public ByteBuffer generateBufferV2() {
        ByteBuffer buf = ByteBuffer.allocate(SEGMENT_LENGTH_V2);
        buf.put(type.code());                  // 0
        buf.put(flags);                        // 1
        buf.putShort((short)0);                // 2-3 reserved
        buf.put(uuid.getBytes());
        buf.putLong(startGno);                 // 68-75
        buf.putLong(cmdStartOffset);           // 4-11
        buf.putLong(blockStartOffset);         // 12-19
        buf.putLong(blockEndOffset);           // 20-27
        buf.putInt(size);                      // 76-79
        buf.putInt(lastCommandLength);         // 80-83
        int crc = crc32c(buf.array(), 0, 84);
        buf.putInt(crc);                       // 84-87
        buf.flip();
        return buf;
    }

    public ByteBuffer generateBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(SEGMENT_LENGTH);
        buffer.put(uuid.getBytes());
        buffer.putLong(startGno);
        buffer.putLong(cmdStartOffset);
        buffer.putLong(blockStartOffset);
        buffer.putLong(blockEndOffset);
        buffer.putInt(size);
        buffer.put(SEPARATOR.getBytes());
        buffer.flip();
        return buffer;
    }

    // ---------- v2 反序列化（CRC 校验）----------
    public static IndexEntry readFromFileV2(FileChannel channel) throws IOException {
        if (fileRemainingLength(channel) < SEGMENT_LENGTH_V2) return null;
        ByteBuffer buf = ByteBuffer.allocate(SEGMENT_LENGTH_V2);
        channel.read(buf);
        buf.flip();

        byte[] body = new byte[84];
        buf.get(body);
        int expectedCrc = crc32c(body, 0, 84);
        int actualCrc = buf.getInt();
        if (expectedCrc != actualCrc) return null;

        ByteBuffer bodyBuf = ByteBuffer.wrap(body);
        IndexEntryType type = IndexEntryType.fromCode(bodyBuf.get());
        byte flags = bodyBuf.get();
        bodyBuf.getShort(); // skip reserved
        long cmdStartOffset = bodyBuf.getLong();
        long blockStartOffset = bodyBuf.getLong();
        long blockEndOffset = bodyBuf.getLong();
        byte[] uuidBytes = new byte[40];
        bodyBuf.get(uuidBytes);
        String uuid = new String(uuidBytes);
        long startGno = bodyBuf.getLong();
        int size = bodyBuf.getInt();
        int lastCommandLength = bodyBuf.getInt();

        IndexEntry e = new IndexEntry(uuid, startGno, cmdStartOffset, blockStartOffset);
        e.type = type;
        e.flags = flags;
        e.blockEndOffset = blockEndOffset;
        e.size = size;
        e.lastCommandLength = lastCommandLength;
        return e;
    }

    public static IndexEntry fromBuffer(ByteBuffer buffer) {
        buffer.flip();
        byte[] uuidBytes = new byte[RedisProtocol.RUN_ID_LENGTH];
        buffer.get(uuidBytes);
        String uuid = new String(uuidBytes);
        long startGno = buffer.getLong();
        long cmdStartOffset = buffer.getLong();
        long blockStartOffset = buffer.getLong();
        long blockEndOffset = buffer.getLong();
        int size = buffer.getInt();

        byte[] separatorBytes = new byte[SEPARATOR.getBytes().length];
        buffer.get(separatorBytes);
        String separator = new String(separatorBytes);
        if (!SEPARATOR.equals(separator)) {
            throw new IllegalArgumentException("Invalid separator in buffer, {}" + separator);
        }
        IndexEntry indexEntry = new IndexEntry(uuid, startGno, cmdStartOffset, blockStartOffset);
        indexEntry.setBlockEndOffset(blockEndOffset);
        indexEntry.setSize(size);
        return indexEntry;
    }

    public void syncDataFromBlockWriter(BlockWriter blockWriter) throws IOException {
        blockWriter.flushBlock();
        this.blockEndOffset = blockWriter.getPosition();
        this.size = blockWriter.getSize();
    }

    // ---------- 写入 ----------
    public void saveToDiskV2(FileChannel channel) throws IOException {
        long pos = channel.position();
        this.position = pos;
        channel.write(generateBufferV2());
    }

    public void saveToDisk(BlockWriter blockWriter, FileChannel channel) throws IOException {
        this.syncDataFromBlockWriter(blockWriter);
        if(position > 0) {
            channel.position(position);
        } else {
            position = channel.position();
        }
        channel.write(generateBuffer());
    }

    private static long fileRemainingLength(FileChannel channel) throws IOException {
        long currentPosition = channel.position();
        long fileSize = channel.size();
        return fileSize - currentPosition;
    }

    public static IndexEntry readFromFile(FileChannel channel) throws IOException {

        if(fileRemainingLength(channel) < SEGMENT_LENGTH) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(SEGMENT_LENGTH);
        channel.read(buffer);
        IndexEntry indexEntry = fromBuffer(buffer);
        return indexEntry;
    }

    private static int crc32c(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "uuid='" + uuid + '\'' +
                ", startGno=" + startGno +
                ", endGno=" + (startGno + size - 1) +
                ", cmdStartOffset=" + cmdStartOffset +
                ", blockStartOffset=" + blockStartOffset +
                ", blockEndOffset=" + blockEndOffset +
                ", size=" + size +
                ", position=" + position +
                '}';
    }

}
