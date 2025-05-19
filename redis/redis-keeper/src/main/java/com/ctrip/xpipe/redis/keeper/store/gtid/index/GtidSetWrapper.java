package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class GtidSetWrapper {

    private GtidSet gtidSet;

    public GtidSet getGtidSet() {
        return gtidSet;
    }

    public void addGtid(String gtid) {
        this.gtidSet.add(gtid);
    }

    public GtidSetWrapper(GtidSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    public void saveGtidSet(FileChannel channel) throws IOException {
        if(channel.size() != 0) {
            throw new IllegalStateException("file channel should be empty before saving GTID set");
        }
        channel.position(0);
        byte[] gtidBytes = gtidSet.toString().getBytes();
        int length = gtidBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(length + Long.BYTES);
        buffer.putLong(length);
        buffer.put(gtidBytes);
        buffer.flip();
        channel.write(buffer);
    }

    static GtidSet readGtidSet(FileChannel channel) throws IOException {
        if(channel.size() < Long.BYTES) {
            throw new IllegalStateException("file channel should be empty before saving GTID set");
        }
        channel.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(buffer);
        buffer.flip();
        long gtidLength = buffer.getLong();
        if(gtidLength == 0) {
            return new GtidSet(Maps.newLinkedHashMap());
        }
        ByteBuffer gtidBuffer = ByteBuffer.allocate((int)gtidLength);
        channel.read(gtidBuffer);
        gtidBuffer.flip();
        String gtidString = new String(gtidBuffer.array());
        GtidSet clone = new GtidSet(gtidString);
        return clone;
    }

    private static long fileRemainingLength(FileChannel channel) throws IOException {
        long currentPosition = channel.position();
        long fileSize = channel.size();
        return fileSize - currentPosition;
    }

    public IndexEntry recover(FileChannel channel) throws IOException {
        channel.position(0);
        IndexEntry indexEntry = null;
        GtidSet recoverGtidSet = GtidSetWrapper.readGtidSet(channel);
        while (fileRemainingLength(channel) >= IndexEntry.SEGMENT_LENGTH) {
            long pos = channel.position();
            IndexEntry item = IndexEntry.readFromFile(channel);
            if(item.getSize() > 0) {
                recoverGtidSet.compensate(item.getUuid(), item.getStartGno(), item.getEndGno());
            }
            item.setPosition(pos);
            indexEntry = item;
        }
        this.gtidSet = recoverGtidSet;
        return indexEntry;
    }

    public void compensate(IndexEntry indexEntry) {
        if(indexEntry != null && indexEntry.getSize() > 0) {
            gtidSet.compensate(indexEntry.getUuid(), indexEntry.getStartGno(), indexEntry.getEndGno());
        }
    }

    public void compensate(IndexEntry unSavedindexEntry, BlockWriter blockWriter) {
        if(unSavedindexEntry != null && blockWriter != null && blockWriter.getSize() > 0) {
            unSavedindexEntry.setSize(blockWriter.getSize());
            compensate(unSavedindexEntry);
        }
    }

    public void compensate(String uuid, long startGno, long endGno) {
        if(endGno >= startGno) {
            gtidSet.compensate(uuid, startGno, endGno);
        }

    }

    private static long readLong(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(buffer);
        buffer.flip();
        return buffer.getLong();
    }

    public static void skipGtidSet(FileChannel channel) throws IOException {
        channel.position(0);
        long gtidLength = readLong(channel);
        channel.position(gtidLength + Long.BYTES);
    }

}
