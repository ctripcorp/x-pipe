package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK_V2;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX_V2;

public class IndexWriterV2 implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IndexWriterV2.class);

    private final Object writeLock = new Object();
    private final DefaultIndexStore store;
    private final AsyncCommandStore cmdStore;
    private final AsyncFileSystem fs;
    private final int zoneConsecutiveThreshold;
    private final long mixedTotalBytesThreshold;
    private final int blockSizeThreshold;

    private BlockEntry currentBlock;
    private IndexEntry currentGtidEntry;
    private GtidSetWrapper gtidSetWrapper;
    private AsyncFile indexV2File;
    private AsyncFile blockV2File;

    private long zoneStart = -1L, zoneEnd = 0L;
    private int zoneCmdCount = 0;
    private long cmdBytesSinceLastIndex = 0L;
    private long gtidCmdEndOffset = 0L;
    private volatile boolean closed;

    public IndexWriterV2(GtidSet gtidSet, DefaultIndexStore store) {
        this(gtidSet, store, 8192, 16L * 1024 * 1024, 8192);
    }

    public IndexWriterV2(GtidSet gtidSet, DefaultIndexStore store,
                         int zoneConsecutiveThreshold, long mixedTotalBytesThreshold, int blockSizeThreshold) {
        this.store = store;
        this.cmdStore = store.getAsyncCommandStore();
        this.fs = cmdStore.getAsyncFileSystem();
        this.gtidSetWrapper = new GtidSetWrapper(gtidSet);
        this.zoneConsecutiveThreshold = zoneConsecutiveThreshold;
        this.mixedTotalBytesThreshold = mixedTotalBytesThreshold;
        this.blockSizeThreshold = blockSizeThreshold;
    }

    public void init(AsyncFile indexV2File, AsyncFile blockV2File) throws IOException {
        this.indexV2File = indexV2File;
        this.blockV2File = blockV2File;
        ensureHeaderIfEmpty();
    }

    void ensureHeaderIfEmpty() throws IOException {
        if (indexV2File == null) {
            return;
        }
        if (AsyncFileSystemHelper.await(fs.size(indexV2File), "size index v2") == 0) {
            gtidSetWrapper.saveGtidSetV2(fs, indexV2File);
        }
    }

    public void appendGtid(String uuid, long gno, long offset, List<Integer> cmdLengths) throws IOException {
        synchronized (writeLock) {
            clearPendingZone();
            int totalLen = 0;
            for (int len : cmdLengths) {
                totalLen += len;
            }
            if (currentBlock != null && currentBlock.needChangeBlock(uuid, gno)) {
                flushGtidEntryUnlocked();
            }
            if (currentBlock == null) {
                createNewBlock(uuid, gno, (int) offset);
            }
            currentBlock.append(uuid, gno, (int) offset);
            gtidCmdEndOffset = offset + totalLen;
            cmdBytesSinceLastIndex += totalLen;
            if (currentBlock.getSize() >= blockSizeThreshold
                    || cmdBytesSinceLastIndex >= mixedTotalBytesThreshold) {
                flushGtidEntryUnlocked();
            }
        }
    }

    public void appendNonGtid(long offset, List<Integer> cmdLengths) throws IOException {
        synchronized (writeLock) {
            int commandCount = cmdLengths.size();
            int totalLen = 0;
            for (int len : cmdLengths) {
                totalLen += len;
            }
            if (zoneStart == -1L) {
                zoneStart = offset;
            }
            zoneEnd = offset + totalLen;
            zoneCmdCount += commandCount;
            cmdBytesSinceLastIndex += totalLen;
            if (zoneCmdCount >= zoneConsecutiveThreshold
                    || cmdBytesSinceLastIndex >= mixedTotalBytesThreshold) {
                flushIndexEntryUnlocked();
            }
        }
    }

    public void flushIndexEntry() throws IOException {
        synchronized (writeLock) {
            flushIndexEntryUnlocked();
        }
    }

    private void flushIndexEntryUnlocked() throws IOException {
        if (hasPendingGtidEntry()) {
            flushGtidEntryUnlocked();
        }
        if (hasPendingZone()) {
            flushZoneUnlocked();
        }
    }

    private boolean hasPendingGtidEntry() {
        return currentGtidEntry != null;
    }

    private boolean hasPendingZone() {
        return zoneStart != -1L && zoneCmdCount > 0;
    }

    private void clearPendingZone() {
        zoneStart = -1L;
        zoneEnd = 0L;
        zoneCmdCount = 0;
    }

    private void flushZoneUnlocked() throws IOException {
        if (!hasPendingZone()) {
            return;
        }
        IndexEntry zoneEntry = IndexEntry.zone(zoneStart, zoneEnd, zoneCmdCount);
        zoneEntry.saveToDiskV2(fs, indexV2File);
        clearPendingZone();
        cmdBytesSinceLastIndex = 0L;
    }

    private void createNewBlock(String uuid, long gno, int cmdOffset) throws IOException {
        this.currentBlock = new BlockEntry(uuid, gno, cmdOffset, blockSizeThreshold);
        long blockStart = AsyncFileSystemHelper.await(fs.size(blockV2File), "size block v2");
        this.currentGtidEntry = new IndexEntry(uuid, gno, cmdOffset, blockStart);
    }

    private void flushGtidEntryUnlocked() throws IOException {
        if (!hasPendingGtidEntry()) {
            return;
        }
        currentGtidEntry.setCmdEndOffset(gtidCmdEndOffset);
        currentGtidEntry.saveToDiskV2(fs, indexV2File, currentBlock, blockV2File);
        gtidSetWrapper.compensate(currentGtidEntry);
        currentBlock = null;
        currentGtidEntry = null;
        cmdBytesSinceLastIndex = 0L;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        synchronized (writeLock) {
            flushIndexEntryUnlocked();
        }
    }

    public GtidSet getGtidSet() {
        synchronized (writeLock) {
            if (currentGtidEntry != null && currentBlock != null) {
                currentGtidEntry.setSize(currentBlock.getSize());
                gtidSetWrapper.compensate(currentGtidEntry);
            }
            return gtidSetWrapper.getGtidSet();
        }
    }

    void recoverIndex(AsyncFile indexV2File, AsyncFile blockV2File) throws IOException {
        resetReadPosition(indexV2File, blockV2File);

        String cmdPrefix = cmdStore.getCommandFileNamePrefix();
        if (!GtidSetWrapper.isV2HeaderComplete(fs, indexV2File)) {
            this.gtidSetWrapper = new GtidSetWrapper(new GtidSet(""));
            currentBlock = null;
            currentGtidEntry = null;
            log.info("[recoverIndex] incomplete v2 header, rebuild from cmd offset 0");
            store.buildIndexFromCmdFile(0, INDEX_V2 + cmdPrefix, BLOCK_V2 + cmdPrefix, 0, 0);
            return;
        }

        long indexSize = AsyncFileSystemHelper.await(fs.size(indexV2File), "size index v2");
        long blockSize = AsyncFileSystemHelper.await(fs.size(blockV2File), "size block v2");
        long cmdSize = cmdStore.currentSegmentSize();

        GtidSetWrapper.V2Header header = GtidSetWrapper.readV2Header(fs, indexV2File);
        GtidSet recoverGtidSet = header.gtidSet;
        long headerEnd = header.headerEnd;
        long lastValidIndexPos = headerEnd;
        long lastValidBlockPos = 0L;
        long rebuildStart = 0L;

        long pos = headerEnd;
        while (indexSize - pos >= IndexEntry.SEGMENT_LENGTH_V2) {
            ByteBuf buf = AsyncFileSystemHelper.await(fs.read(indexV2File, IndexEntry.SEGMENT_LENGTH_V2, pos),
                    "read index v2 entry");
            IndexEntry indexEntry;
            try {
                indexEntry = IndexEntry.fromBufferV2(buf);
            } finally {
                buf.release();
            }
            if (indexEntry == null) {
                break;
            }

            boolean valid;
            if (indexEntry.isZone()) {
                valid = indexEntry.getZoneEnd() <= cmdSize;
            } else {
                valid = indexEntry.getBlockEndOffset() != -1
                        && indexEntry.getCmdEndOffset() <= cmdSize
                        && indexEntry.getBlockEndOffset() <= blockSize;
            }
            if (!valid) {
                break;
            }

            if (indexEntry.isZone()) {
                rebuildStart = Math.max(rebuildStart, indexEntry.getZoneEnd());
            } else {
                if (indexEntry.getSize() > 0) {
                    recoverGtidSet.compensate(indexEntry.getUuid(), indexEntry.getStartGno(), indexEntry.getEndGno());
                }
                rebuildStart = Math.max(rebuildStart, indexEntry.getCmdEndOffset());
            }
            pos += IndexEntry.SEGMENT_LENGTH_V2;
            lastValidIndexPos = pos;
            if (!indexEntry.isZone()) {
                lastValidBlockPos = indexEntry.getBlockEndOffset();
            }
        }

        this.gtidSetWrapper = new GtidSetWrapper(recoverGtidSet);
        currentBlock = null;
        currentGtidEntry = null;

        log.info("[recoverIndex] rebuildStart={}, lastValidIndexPos={}, lastValidBlockPos={}, cmdSize={}",
                rebuildStart, lastValidIndexPos, lastValidBlockPos, cmdSize);
        store.buildIndexFromCmdFile(rebuildStart, INDEX_V2 + cmdPrefix, BLOCK_V2 + cmdPrefix,
                lastValidIndexPos, lastValidBlockPos);
    }

    private void resetReadPosition(AsyncFile indexFile, AsyncFile blockFile) throws IOException {
        AsyncFileSystemHelper.await(fs.position(indexFile, 0), "position index v2 to start for recover");
        AsyncFileSystemHelper.await(fs.position(blockFile, 0), "position block v2 to start for recover");
    }

    /** Test helper: load ZONE intervals from index file via a short-life read-mode segment. */
    List<long[]> loadAllZones() throws IOException {
        String cmdPrefix = cmdStore.getCommandFileNamePrefix();
        List<String> prefixes = List.of(INDEX_V2 + cmdPrefix, BLOCK_V2 + cmdPrefix);
        AsyncSegmentFile readSeg = store.openReadSegment(prefixes);
        try {
            long segStart = cmdStore.getCurrentSegmentStartOffset();
            AsyncFileSystemHelper.await(fs.position(readSeg, segStart), "position read segment for loadAllZones");
            Map<String, AsyncFile> readHandles = AsyncFileSystemHelper.await(
                    fs.getCurrentIndexFiles(readSeg, prefixes), "get read index handles for loadAllZones");
            AsyncFile readIndexV2 = readHandles.get(INDEX_V2 + cmdPrefix);
            return scanZones(readIndexV2);
        } finally {
            AsyncFileSystemHelper.await(fs.close(readSeg), "close read segment for loadAllZones");
        }
    }

    private List<long[]> scanZones(AsyncFile readIndexV2) throws IOException {
        List<long[]> zones = new ArrayList<>();
        long headerEnd = GtidSetWrapper.headerSize(fs, readIndexV2);
        long indexSize = AsyncFileSystemHelper.await(fs.size(readIndexV2), "size index v2");
        long cmdSize = cmdStore.currentSegmentSize();
        long pos = headerEnd;
        while (indexSize - pos >= IndexEntry.SEGMENT_LENGTH_V2) {
            ByteBuf buf = AsyncFileSystemHelper.await(fs.read(readIndexV2, IndexEntry.SEGMENT_LENGTH_V2, pos),
                    "read zone entry");
            try {
                IndexEntry indexEntry = IndexEntry.fromBufferV2(buf);
                if (indexEntry == null) {
                    break;
                }
                if (indexEntry.isZone() && indexEntry.getZoneEnd() <= cmdSize) {
                    zones.add(new long[]{indexEntry.getZoneStart(), indexEntry.getZoneEnd()});
                }
            } finally {
                buf.release();
            }
            pos += IndexEntry.SEGMENT_LENGTH_V2;
        }
        return zones;
    }
}
