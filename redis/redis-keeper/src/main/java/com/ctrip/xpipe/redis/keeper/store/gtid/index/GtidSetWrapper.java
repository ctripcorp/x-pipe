package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;

public class GtidSetWrapper {

    public static final int MAGIC = 0x47494458;   // "GIDX"
    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2;

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

    public void saveGtidSet(AsyncFileSystem fs, AsyncFile file) throws IOException {
        if (AsyncFileSystemHelper.await(fs.size(file), "size index") != 0) {
            throw new IllegalStateException("index file should be empty before saving GTID set");
        }
        byte[] gtidBytes = gtidSet.toString().getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(gtidBytes.length + Long.BYTES);
        buffer.putLong(gtidBytes.length);
        buffer.put(gtidBytes);
        buffer.flip();
        ByteBuf buf = Unpooled.wrappedBuffer(buffer).retain();
        try {
            AsyncFileSystemHelper.writeAndAwait(fs, file, buf, buffer.remaining(), "write gtid set v1 header");
        } finally {
            buf.release();
        }
    }

    public void saveGtidSetV2(AsyncFileSystem fs, AsyncFile file) throws IOException {
        byte[] gtidBytes = gtidSet.toString().getBytes();
        ByteBuffer header = ByteBuffer.allocate(16 + gtidBytes.length);
        header.putInt(MAGIC);
        header.putInt(VERSION_2);
        header.putLong(gtidBytes.length);
        header.put(gtidBytes);
        header.flip();
        ByteBuf buf = Unpooled.wrappedBuffer(header).retain();
        try {
            AsyncFileSystemHelper.writeAndAwait(fs, file, buf, header.remaining(), "write gtid set v2 header");
        } finally {
            buf.release();
        }
    }

    public static boolean isV1HeaderComplete(AsyncFileSystem fs, AsyncFile file) throws IOException {
        long size = AsyncFileSystemHelper.await(fs.size(file), "size index v1");
        if (size < Long.BYTES) {
            return false;
        }
        ByteBuf lenBuf = AsyncFileSystemHelper.await(fs.read(file, Long.BYTES, 0), "read v1 header len");
        try {
            long gtidLen = lenBuf.readLong();
            return gtidLen >= 0 && size >= Long.BYTES + gtidLen;
        } finally {
            lenBuf.release();
        }
    }

    public static boolean isV2HeaderComplete(AsyncFileSystem fs, AsyncFile file) throws IOException {
        long size = AsyncFileSystemHelper.await(fs.size(file), "size index v2");
        if (size < 16) {
            return false;
        }
        ByteBuf headerBuf = AsyncFileSystemHelper.await(fs.read(file, 16, 0), "read v2 header prefix");
        try {
            if (headerBuf.readInt() != MAGIC) {
                return false;
            }
            headerBuf.readInt();
            long gtidLen = headerBuf.readLong();
            return gtidLen >= 0 && size >= 16 + gtidLen;
        } finally {
            headerBuf.release();
        }
    }

    public static GtidSet readGtidSet(AsyncFileSystem fs, AsyncFile file) throws IOException {
        long size = AsyncFileSystemHelper.await(fs.size(file), "size index v1");
        if (size < Long.BYTES) {
            throw new IllegalStateException("index file too small for v1 header");
        }
        ByteBuf buf = AsyncFileSystemHelper.await(fs.read(file, Long.BYTES, 0), "read gtid set v1 length");
        try {
            long gtidLength = buf.readLong();
            if (gtidLength == 0) {
                return new GtidSet(Maps.newLinkedHashMap());
            }
            byte[] data = AsyncFileSystemHelper.readAllBytes(fs, file, gtidLength, Long.BYTES, "read gtid set v1");
            return new GtidSet(new String(data));
        } finally {
            buf.release();
        }
    }

    public static GtidSet readGtidSetV2(AsyncFileSystem fs, AsyncFile file) throws IOException {
        return readV2Header(fs, file).gtidSet;
    }

    public static final class V2Header {
        public final GtidSet gtidSet;
        public final long headerEnd;

        V2Header(GtidSet gtidSet, long headerEnd) {
            this.gtidSet = gtidSet;
            this.headerEnd = headerEnd;
        }
    }

    public static V2Header readV2Header(AsyncFileSystem fs, AsyncFile file) throws IOException {
        long size = AsyncFileSystemHelper.await(fs.size(file), "size index v2");
        if (size < 16) {
            throw new IOException("Index file too small");
        }
        ByteBuf headerBuf = AsyncFileSystemHelper.await(fs.read(file, 16, 0), "read gtid set v2 header");
        try {
            if (headerBuf.readInt() != MAGIC) {
                throw new IllegalStateException("Not a valid v2 index file (bad magic)");
            }
            headerBuf.readInt();
            long gtidLen = headerBuf.readLong();
            GtidSet gtidSet;
            if (gtidLen == 0) {
                gtidSet = new GtidSet(Maps.newLinkedHashMap());
            } else {
                byte[] data = AsyncFileSystemHelper.readAllBytes(fs, file, gtidLen, 16, "read gtid set v2 body");
                gtidSet = new GtidSet(new String(data));
            }
            return new V2Header(gtidSet, 16 + gtidLen);
        } finally {
            headerBuf.release();
        }
    }

    public static long headerSize(AsyncFileSystem fs, AsyncFile file) throws IOException {
        long size = AsyncFileSystemHelper.await(fs.size(file), "size index v2 header");
        if (size < 4) {
            return 0L;
        }
        ByteBuf magicBuf = AsyncFileSystemHelper.await(fs.read(file, 4, 0), "read index magic");
        try {
            int firstInt = magicBuf.readInt();
            if (firstInt == MAGIC) {
                ByteBuf lenBuf = AsyncFileSystemHelper.await(fs.read(file, Long.BYTES, 8), "read v2 gtid length");
                try {
                    long gtidLen = lenBuf.readLong();
                    return 16 + gtidLen;
                } finally {
                    lenBuf.release();
                }
            }
            ByteBuf lenBuf = AsyncFileSystemHelper.await(fs.read(file, Long.BYTES, 0), "read v1 gtid length");
            try {
                long gtidLen = lenBuf.readLong();
                return 8 + gtidLen;
            } finally {
                lenBuf.release();
            }
        } finally {
            magicBuf.release();
        }
    }

    public IndexEntry recover(AsyncFileSystem fs, AsyncFile indexFile) throws IOException {
        GtidSet recoverGtidSet = readGtidSet(fs, indexFile);
        ByteBuf lenBuf = AsyncFileSystemHelper.await(fs.read(indexFile, Long.BYTES, 0), "read v1 header len");
        long gtidLength;
        try {
            gtidLength = lenBuf.readLong();
        } finally {
            lenBuf.release();
        }
        long headerEnd = Long.BYTES + gtidLength;
        long fileSize = AsyncFileSystemHelper.await(fs.size(indexFile), "size index v1");
        IndexEntry indexEntry = null;
        long pos = headerEnd;
        while (fileSize - pos >= IndexEntry.SEGMENT_LENGTH) {
            ByteBuf entryBuf = AsyncFileSystemHelper.await(fs.read(indexFile, IndexEntry.SEGMENT_LENGTH, pos),
                    "read index v1 entry");
            try {
                IndexEntry item = IndexEntry.fromBuffer(entryBuf);
                if (item == null) {
                    break;
                }
                if (item.getSize() > 0) {
                    recoverGtidSet.compensate(item.getUuid(), item.getStartGno(), item.getEndGno());
                }
                item.setPosition(pos);
                indexEntry = item;
            } finally {
                entryBuf.release();
            }
            pos += IndexEntry.SEGMENT_LENGTH;
        }
        this.gtidSet = recoverGtidSet;
        return indexEntry;
    }

    public void compensate(IndexEntry indexEntry) {
        if (indexEntry != null && !indexEntry.isZone() && indexEntry.getSize() > 0) {
            gtidSet.compensate(indexEntry.getUuid(), indexEntry.getStartGno(), indexEntry.getEndGno());
        }
    }

    public void compensate(IndexEntry unSavedindexEntry, BlockEntry blockEntry) {
        if (unSavedindexEntry != null && !unSavedindexEntry.isZone() && blockEntry != null && blockEntry.getSize() > 0) {
            unSavedindexEntry.setSize(blockEntry.getSize());
            compensate(unSavedindexEntry);
        }
    }

    public void compensate(String uuid, long startGno, long endGno) {
        if (endGno >= startGno) {
            gtidSet.compensate(uuid, startGno, endGno);
        }
    }
}
