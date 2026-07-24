package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class IndexReaderV2 extends IndexReader {

    public IndexReaderV2(AsyncFileSystem fs, String baseDir, String cmdPrefix, long segmentStartOffset, ReplId tenant)
            throws IOException {
        super(fs, baseDir, cmdPrefix, segmentStartOffset, tenant,
                List.of(AbstractIndex.INDEX_V2 + cmdPrefix, AbstractIndex.BLOCK_V2 + cmdPrefix));
    }

    @Override
    protected String getIndexKey() {
        return AbstractIndex.INDEX_V2 + cmdPrefix;
    }

    @Override
    protected String getBlockKey() {
        return AbstractIndex.BLOCK_V2 + cmdPrefix;
    }

    @Override
    protected int getSegmentLength() {
        return IndexEntry.SEGMENT_LENGTH_V2;
    }

    @Override
    protected GtidSet readStartGtidSet() throws IOException {
        return GtidSetWrapper.readGtidSetV2(fs, indexFile);
    }

    @Override
    protected long headerEndPosition() throws IOException {
        return GtidSetWrapper.headerSize(fs, indexFile);
    }

    @Override
    protected IndexEntry decodeEntry(ByteBuffer buffer) {
        return IndexEntry.fromBufferV2(buffer);
    }

    @Override
    protected boolean isHeaderComplete() throws IOException {
        return GtidSetWrapper.isV2HeaderComplete(fs, indexFile);
    }

    @Override
    public void init() throws IOException {
        indexItemList = new ArrayList<>();
        AsyncFileSystemHelper.await(fs.position(readSeg, segmentStartOffset), "position read segment v2");
        var handles = AsyncFileSystemHelper.await(fs.getCurrentIndexFiles(readSeg, indexPrefixes), "get index v2 files");
        indexFile = handles.get(getIndexKey());
        blockFile = handles.get(getBlockKey());

        long size = AsyncFileSystemHelper.await(fs.size(indexFile), "size index v2 file");
        if (size == 0) {
            startGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
            return;
        }
        if (!isHeaderComplete()) {
            startGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
            return;
        }
        startGtidSet = readStartGtidSet();
        long pos = headerEndPosition();
        while (size - pos >= getSegmentLength()) {
            ByteBuf buf = AsyncFileSystemHelper.await(fs.read(indexFile, getSegmentLength(), pos), "read index v2 entry");
            try {
                IndexEntry item = decodeEntry(buf.nioBuffer());
                if (item == null) {
                    break;
                }
                if (!item.isZone()) {
                    indexItemList.add(item);
                }
            } finally {
                buf.release();
            }
            pos += getSegmentLength();
        }
    }

    public static IndexReader getLastIndexReader(AsyncFileSystem fs, String baseDir, String cmdPrefix, ReplId tenant)
            throws IOException {
        return getFloorIndexReader(fs, baseDir, cmdPrefix, tenant, -1, AbstractIndex.INDEX_V2 + cmdPrefix);
    }

    public static IndexReader getFirstIndexReader(AsyncFileSystem fs, String baseDir, String cmdPrefix, ReplId tenant)
            throws IOException {
        AsyncSegmentFile tempSeg = openTempReadSeg(fs, baseDir, cmdPrefix, tenant,
                List.of(AbstractIndex.INDEX_V2 + cmdPrefix, AbstractIndex.BLOCK_V2 + cmdPrefix));
        try {
            List<Long> offsets = fs.list(tempSeg);
            if (offsets.isEmpty()) {
                return null;
            }
            return new IndexReaderV2(fs, baseDir, cmdPrefix, offsets.get(0), tenant);
        } finally {
            AsyncFileSystemHelper.await(fs.close(tempSeg), "close temp read segment v2");
        }
    }
}
