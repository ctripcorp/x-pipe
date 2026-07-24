package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class IndexReader implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(IndexReader.class);

    protected final AsyncFileSystem fs;
    protected final String baseDir;
    protected final String cmdPrefix;
    protected long segmentStartOffset;
    protected final ReplId tenant;
    protected final AsyncSegmentFile readSeg;
    protected final List<String> indexPrefixes;

    protected List<IndexEntry> indexItemList;
    protected GtidSet startGtidSet;
    protected AsyncFile indexFile;
    protected AsyncFile blockFile;
    protected long cmdStoreStartOffset;

    public void setCmdStoreStartOffset(long cmdStoreStartOffset) {
        this.cmdStoreStartOffset = Math.max(0L, cmdStoreStartOffset);
    }

    public IndexReader(AsyncFileSystem fs, String baseDir, String cmdPrefix, long segmentStartOffset, ReplId tenant)
            throws IOException {
        this(fs, baseDir, cmdPrefix, segmentStartOffset, tenant,
                List.of(AbstractIndex.INDEX + cmdPrefix, AbstractIndex.BLOCK + cmdPrefix));
    }

    protected IndexReader(AsyncFileSystem fs, String baseDir, String cmdPrefix, long segmentStartOffset,
                          ReplId tenant, List<String> indexPrefixes) throws IOException {
        this.fs = fs;
        this.baseDir = baseDir;
        this.cmdPrefix = cmdPrefix;
        this.segmentStartOffset = segmentStartOffset;
        this.tenant = tenant;
        this.indexPrefixes = indexPrefixes;
        this.readSeg = AsyncFileSystemHelper.await(
                fs.open(baseDir, cmdPrefix, indexPrefixes, false, tenant.toString()),
                "open read segment for index");
    }

    protected String getIndexKey() {
        return AbstractIndex.INDEX + cmdPrefix;
    }

    protected String getBlockKey() {
        return AbstractIndex.BLOCK + cmdPrefix;
    }

    protected int getSegmentLength() {
        return IndexEntry.SEGMENT_LENGTH;
    }

    protected GtidSet readStartGtidSet() throws IOException {
        return GtidSetWrapper.readGtidSet(fs, indexFile);
    }

    protected long headerEndPosition() throws IOException {
        ByteBuf lenBuf = AsyncFileSystemHelper.await(fs.read(indexFile, Long.BYTES, 0), "read v1 header length");
        try {
            long gtidLength = lenBuf.readLong();
            return Long.BYTES + gtidLength;
        } finally {
            lenBuf.release();
        }
    }

    protected IndexEntry decodeEntry(ByteBuffer buffer) {
        return IndexEntry.fromBuffer(buffer);
    }

    protected boolean isHeaderComplete() throws IOException {
        return GtidSetWrapper.isV1HeaderComplete(fs, indexFile);
    }

    public void init() throws IOException {
        indexItemList = new ArrayList<>();
        AsyncFileSystemHelper.await(fs.position(readSeg, segmentStartOffset), "position read segment");
        var handles = AsyncFileSystemHelper.await(fs.getCurrentIndexFiles(readSeg, indexPrefixes), "get index files");
        indexFile = handles.get(getIndexKey());
        blockFile = handles.get(getBlockKey());

        long size = AsyncFileSystemHelper.await(fs.size(indexFile), "size index file");
        if (size == 0) {
            log.warn("[IndexReader] file length is 0");
            startGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
            return;
        }
        if (!isHeaderComplete()) {
            log.warn("[IndexReader] incomplete index header at segment {}", segmentStartOffset);
            startGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
            return;
        }
        startGtidSet = readStartGtidSet();
        long pos = headerEndPosition();
        while (size - pos >= getSegmentLength()) {
            ByteBuf buf = AsyncFileSystemHelper.await(fs.read(indexFile, getSegmentLength(), pos), "read index entry");
            try {
                IndexEntry item = decodeEntry(buf.nioBuffer());
                if (item == null) {
                    break;
                }
                indexItemList.add(item);
            } finally {
                buf.release();
            }
            pos += getSegmentLength();
        }
    }

    public GtidSet getStartGtidSet() {
        return startGtidSet;
    }

    public String getFileName() {
        return cmdPrefix + segmentStartOffset;
    }

    public long getStartOffset() {
        return segmentStartOffset;
    }

    /** Used by IndexReaderTest only. */
    public long seek(String uuid, long gno) throws IOException {
        int index = -1;
        for (int i = indexItemList.size() - 1; i >= 0; i--) {
            IndexEntry item = indexItemList.get(i);
            if (!StringUtil.trimEquals(item.getUuid(), uuid)) {
                continue;
            }
            if (gno >= item.getStartGno() && gno <= item.getEndGno()) {
                index = i;
                break;
            }
        }
        IndexEntry item = indexItemList.get(index);
        if (gno == item.getStartGno()) {
            return item.getCmdStartOffset();
        }
        int arrayIndex = (int) gno - (int) item.getStartGno();
        return BlockReader.seek(fs, blockFile, item.getBlockStartOffset(), item.getBlockEndOffset(), arrayIndex)
                + item.getCmdStartOffset();
    }

    public Pair<Long, GtidSet> seek(GtidSet request) throws IOException {
        if (segmentStartOffset < cmdStoreStartOffset) {
            return new Pair<>(-1L, new GtidSet(GtidSet.EMPTY_GTIDSET));
        }
        long offset = -1L;
        GtidSet gtidSet = null;
        boolean changeFileSuccess = true;
        boolean finish = false;
        while (changeFileSuccess) {
            for (int i = indexItemList.size() - 1; i >= 0; i--) {
                IndexEntry indexEntry = indexItemList.get(i);
                if (indexEntry.getBlockEndOffset() == -1) {
                    continue;
                }
                GtidSet.UUIDSet uuidSet = request.getUUIDSet(indexEntry.getUuid());
                long nexGno = uuidSet != null ? uuidSet.getLastGno() + 1 : GtidSet.GTID_GNO_INITIAL;

                if (nexGno > indexEntry.getEndGno()) {
                    finish = true;
                    if (gtidSet == null) {
                        gtidSet = calculateGtidSet(i + 1);
                    }
                    break;
                } else if (nexGno >= indexEntry.getStartGno() && nexGno <= indexEntry.getEndGno()) {
                    long index = nexGno - indexEntry.getStartGno();
                    offset = BlockReader.seek(fs, blockFile, indexEntry.getBlockStartOffset(),
                            indexEntry.getBlockEndOffset(), (int) index) + indexEntry.getCmdStartOffset() + getStartOffset();
                    gtidSet = calculateGtidSet(i, nexGno - 1);
                    finish = true;
                    break;
                } else if (nexGno < indexEntry.getStartGno()) {
                    offset = indexEntry.getCmdStartOffset() + getStartOffset();
                    gtidSet = calculateGtidSet(i);
                }
            }
            if (finish) {
                break;
            }
            if (startGtidSet.isEmpty()) {
                break;
            }
            if (!canChangeToPre()) {
                break;
            }
            changeFileSuccess = changeToPre();
        }
        if (gtidSet == null) {
            gtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
        }
        return new Pair<>(offset, gtidSet);
    }

    private boolean canChangeToPre() throws IOException {
        if (segmentStartOffset <= cmdStoreStartOffset) {
            return false;
        }
        List<Long> offsets = fs.list(readSeg);
        Long pre = offsets.stream().filter(o -> o < segmentStartOffset).max(Long::compare).orElse(null);
        return pre != null && pre >= cmdStoreStartOffset;
    }

    public boolean noIndex() {
        return indexItemList.isEmpty();
    }

    private GtidSet calculateGtidSet(int index, long gno) {
        GtidSet startGtid = new GtidSet(getStartGtidSet().toString());
        GtidSetWrapper gtidSetWrapper = new GtidSetWrapper(startGtid);
        for (int i = 0; i < index; i++) {
            IndexEntry indexEntry = indexItemList.get(i);
            gtidSetWrapper.compensate(indexEntry.getUuid(), indexEntry.getStartGno(), indexEntry.getEndGno());
        }
        IndexEntry indexEntry = indexItemList.get(index);
        gno = Math.min(gno, indexEntry.getEndGno());
        gtidSetWrapper.compensate(indexEntry.getUuid(), indexEntry.getStartGno(), gno);
        return gtidSetWrapper.getGtidSet();
    }

    private GtidSet calculateGtidSet(int index) {
        GtidSet startGtid = new GtidSet(getStartGtidSet().toString());
        GtidSetWrapper gtidSetWrapper = new GtidSetWrapper(startGtid);
        for (int i = 0; i < index; i++) {
            IndexEntry indexEntry = indexItemList.get(i);
            gtidSetWrapper.compensate(indexEntry.getUuid(), indexEntry.getStartGno(), indexEntry.getEndGno());
        }
        return gtidSetWrapper.getGtidSet();
    }

    public GtidSet getAllGtidSet() {
        return calculateGtidSet(indexItemList.size());
    }

    public boolean changeToPre() throws IOException {
        List<Long> offsets = fs.list(readSeg);
        Long pre = offsets.stream().filter(o -> o < segmentStartOffset).max(Long::compare).orElse(null);
        if (pre == null) {
            return false;
        }
        segmentStartOffset = pre;
        init();
        return true;
    }

    public boolean changeToNext() throws IOException {
        List<Long> offsets = fs.list(readSeg);
        Long next = offsets.stream().filter(o -> o > segmentStartOffset).min(Long::compare).orElse(null);
        if (next == null) {
            return false;
        }
        segmentStartOffset = next;
        init();
        return true;
    }

    public Long findNextSegmentOffset() throws IOException {
        List<Long> offsets = fs.list(readSeg);
        return offsets.stream().filter(o -> o > segmentStartOffset).min(Long::compare).orElse(null);
    }

    public static IndexReader getLastIndexReader(AsyncFileSystem fs, String baseDir, String cmdPrefix, ReplId tenant)
            throws IOException {
        return getFloorIndexReader(fs, baseDir, cmdPrefix, tenant, -1, AbstractIndex.INDEX + cmdPrefix);
    }

    public static IndexReader getFirstIndexReader(AsyncFileSystem fs, String baseDir, String cmdPrefix, ReplId tenant)
            throws IOException {
        AsyncSegmentFile tempSeg = openTempReadSeg(fs, baseDir, cmdPrefix, tenant,
                List.of(AbstractIndex.INDEX + cmdPrefix, AbstractIndex.BLOCK + cmdPrefix));
        try {
            List<Long> offsets = fs.list(tempSeg);
            if (offsets.isEmpty()) {
                return null;
            }
            return new IndexReader(fs, baseDir, cmdPrefix, offsets.get(0), tenant);
        } finally {
            AsyncFileSystemHelper.await(fs.close(tempSeg), "close temp read segment");
        }
    }

    protected static IndexReader getFloorIndexReader(AsyncFileSystem fs, String baseDir, String cmdPrefix,
                                                     ReplId tenant, long currentOffset, String indexPrefix)
            throws IOException {
        AsyncSegmentFile tempSeg = openTempReadSeg(fs, baseDir, cmdPrefix, tenant,
                List.of(indexPrefix, indexPrefix.replace(AbstractIndex.INDEX, AbstractIndex.BLOCK)));
        try {
            List<Long> offsets = fs.list(tempSeg);
            if (offsets.isEmpty()) {
                return null;
            }
            Long target = offsets.stream()
                    .filter(o -> o < currentOffset || currentOffset < 0)
                    .max(Long::compare)
                    .orElse(null);
            if (target == null) {
                return null;
            }
            if (indexPrefix.startsWith(AbstractIndex.INDEX_V2)) {
                return new IndexReaderV2(fs, baseDir, cmdPrefix, target, tenant);
            }
            return new IndexReader(fs, baseDir, cmdPrefix, target, tenant);
        } finally {
            AsyncFileSystemHelper.await(fs.close(tempSeg), "close temp read segment");
        }
    }

    protected static AsyncSegmentFile openTempReadSeg(AsyncFileSystem fs, String baseDir, String cmdPrefix,
                                                      ReplId tenant, List<String> prefixes) throws IOException {
        return AsyncFileSystemHelper.await(
                fs.open(baseDir, cmdPrefix, prefixes, false, tenant.toString()),
                "open temp read segment");
    }

    public List<Pair<Long, Long>> findMatchingRanges(String uuid, long begGno, long endGno) throws IOException {
        List<Pair<Long, Long>> ranges = new ArrayList<>();
        for (int i = 0; i < indexItemList.size(); i++) {
            IndexEntry item = indexItemList.get(i);
            if (!StringUtil.trimEquals(item.getUuid(), uuid)) {
                continue;
            }
            long entryStartGno = item.getStartGno();
            long entryEndGno = item.getEndGno();
            if (entryEndGno < begGno || entryStartGno > endGno) {
                continue;
            }
            if (item.getBlockEndOffset() == -1) {
                continue;
            }
            long rangeStartGno = Math.max(entryStartGno, begGno);
            long rangeEndGno = Math.min(entryEndGno, endGno);
            long startOffset = calculateOffsetForGno(item, rangeStartGno);
            Long endOffset;
            if (rangeEndGno < item.getEndGno()) {
                endOffset = calculateOffsetForGno(item, rangeEndGno + 1);
            } else if (i + 1 < indexItemList.size()) {
                endOffset = indexItemList.get(i + 1).getCmdStartOffset();
            } else {
                endOffset = null;
            }
            ranges.add(new Pair<>(startOffset, endOffset));
        }
        return ranges;
    }

    private long calculateOffsetForGno(IndexEntry item, long gno) throws IOException {
        if (gno == item.getStartGno()) {
            return item.getCmdStartOffset();
        }
        int arrayIndex = (int) (gno - item.getStartGno());
        return BlockReader.seek(fs, blockFile, item.getBlockStartOffset(), item.getBlockEndOffset(), arrayIndex)
                + item.getCmdStartOffset();
    }

    @Override
    public void close() throws IOException {
        AsyncFileSystemHelper.await(fs.close(readSeg), "close read segment");
    }
}
