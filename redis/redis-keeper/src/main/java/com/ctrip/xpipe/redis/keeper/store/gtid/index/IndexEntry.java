package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IndexEntry {

    private String uuid;
    private long startGno;
    private long cmdStartOffset;
    private long blockStartOffset;
    private long blockEndOffset;
    private int size;

    private static final String SEPARATOR = RedisProtocol.CRLF;

    public static final int SEGMENT_LENGTH = RedisProtocol.RUN_ID_LENGTH + Long.BYTES * 4 + SEPARATOR.getBytes().length + Integer.BYTES;

    private long position;

    private FileChannel channel;

    public FileChannel getChannel() {
        return channel;
    }

    public void setChannel(FileChannel channel) {
        this.channel = channel;
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

    public IndexEntry(String uuid, long startGno, long cmdStartOffset, long blockStartOffset) {
        this.uuid = uuid;
        this.startGno = startGno;
        this.cmdStartOffset = cmdStartOffset;
        this.blockStartOffset = blockStartOffset;
        this.blockEndOffset = -1;
        this.position = -1;
        this.channel = null;
        this.size = 0;
        // -1 表示没有写完
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

        byte[] separatorBytes = new byte[SEPARATOR.length()];
        buffer.get(separatorBytes);
        String separator = new String(separatorBytes);
        if (!SEPARATOR.equals(separator)) {
            throw new IllegalArgumentException("Invalid separator in buffer");
        }
        IndexEntry indexEntry = new IndexEntry(uuid, startGno, cmdStartOffset, blockStartOffset);
        indexEntry.setBlockEndOffset(blockEndOffset);
        indexEntry.setSize(size);
        return indexEntry;
    }

    public void syncDataFromBlockWriter(BlockWriter blockWriter) throws IOException {
        this.blockEndOffset = blockWriter.getPosition();
        this.size = blockWriter.getSize();
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
        indexEntry.setChannel(channel);
        return indexEntry;
    }

    public IndexEntry changeToPre() throws IOException {
        long preIndex = position - SEGMENT_LENGTH;
        if(preIndex <= 0) {
            return null;
        }
        channel.position(preIndex);
        IndexEntry result = IndexEntry.readFromFile(channel);
        result.setPosition(preIndex);
        return result;
    }

}
