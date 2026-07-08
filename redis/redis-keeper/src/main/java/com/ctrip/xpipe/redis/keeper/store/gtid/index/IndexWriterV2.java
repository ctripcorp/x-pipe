package com.ctrip.xpipe.redis.keeper.store.gtid.index;


import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author TB
 * @date 2026/6/29 20:42
 */

public class IndexWriterV2 extends AbstractIndex implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IndexWriterV2.class);

    private final Object writeLock = new Object();

    private BlockWriter blockWriter;
    private IndexEntry currentGtidEntry;
    private GtidSetWrapper gtidSetWrapper;
    private DefaultIndexStore store;
    private ByteBuffer byteBuffer;

    private long zoneStart = -1L, zoneEnd = 0L;
    private int zoneCmdCount = 0;
    private long cmdBytesSinceLastIndex = 0L;
    /** 当前 pending GTID entry 在 cmd 文件上的结束 offset，仅 appendGtid 更新 */
    private long gtidCmdEndOffset = 0L;

    private final int zoneConsecutiveThreshold;
    private final long mixedTotalBytesThreshold;
    private final int blockSizeThreshold;

    public IndexWriterV2(String baseDir, String cmdFileName, GtidSet gtidSet, DefaultIndexStore store) {
        this(baseDir, cmdFileName, gtidSet, store, 8192, 16L * 1024 * 1024,8192);
    }

    public IndexWriterV2(String baseDir, String cmdFileName, GtidSet gtidSet, DefaultIndexStore store,
                         int zoneConsecutiveThreshold, long mixedTotalBytesThreshold,int blockSizeThreshold) {
        super(baseDir, cmdFileName);
        this.gtidSetWrapper = new GtidSetWrapper(gtidSet);
        this.store = store;
        this.zoneConsecutiveThreshold = zoneConsecutiveThreshold;
        this.mixedTotalBytesThreshold = mixedTotalBytesThreshold;
        this.blockSizeThreshold = blockSizeThreshold;
    }

    @Override protected String getIndexPrefix() { return INDEX_V2; }
    @Override protected String getBlockPrefix() { return BLOCK_V2; }

    @Override
    public void init() throws IOException {
        super.initIndexFile();
        byteBuffer = ByteBuffer.allocateDirect(32768);
        if (indexFile.getFileChannel().size() == 0) {
            gtidSetWrapper.saveGtidSetV2(indexFile.getFileChannel());
        } else {
            recoverIndex();
        }
    }

    // GTID 事务写入：整批一次
    public void appendGtid(String uuid, long gno, long offset, List<Integer> cmdLengths) throws IOException {
        synchronized (writeLock) {
            // R1/R2: GTID 到来 → 清空 pending zone 状态（不落 ZONE）
            clearPendingZone();

            int totalLen = 0;
            for (int len : cmdLengths) {
                totalLen += len;
            }

            // block 满或 uuid/gno gap：append 前先落盘当前 block（与 v1 IndexWriter 一致）
            if (needChangeBlock(uuid, gno)) {
                flushGtidEntryUnlocked();
            }
            if (blockWriter == null) {
                createNewBlock(uuid, gno, (int) offset);
            }
            blockWriter.append(uuid, gno, (int) offset);

            gtidCmdEndOffset = offset + totalLen;
            cmdBytesSinceLastIndex += totalLen;

            // 统一判断：block 满 or 累计字节达到强制滚动阈值 → 落 GTID entry
            if (blockWriter.getSize() >= blockSizeThreshold || cmdBytesSinceLastIndex >= mixedTotalBytesThreshold) {
                flushGtidEntryUnlocked();
            }
        }
    }

    // 非 GTID 事务/单条写入：仅扩展 zone
    public void appendNonGtid(long offset, List<Integer> cmdLengths) throws IOException {
        synchronized (writeLock) {
            int commandCount = cmdLengths.size();
            int totalLen = 0;
            for (int len : cmdLengths) {
                totalLen += len;
            }

            if (zoneStart == -1L) zoneStart = offset;
            zoneEnd = offset + totalLen;
            zoneCmdCount += commandCount;
            cmdBytesSinceLastIndex += totalLen;

            // 统一判断：连续 non-GTID 达到条数阈值 or 累计字节达到强制滚动阈值 → flush
            if (zoneCmdCount >= zoneConsecutiveThreshold
                    || cmdBytesSinceLastIndex >= mixedTotalBytesThreshold) {
                flushIndexEntryUnlocked();
            }
        }
    }

    private boolean needChangeBlock(String uuid, long gno) {
        if(blockWriter == null) return false;
        return blockWriter.getSize() >= blockSizeThreshold  || blockWriter.isGnoGap(uuid, gno);
    }


    /**
     * 落盘当前 pending 的 IndexEntry。GTID 与 ZONE 同时存在时先 GTID 后 ZONE；否则落存在的一方。
     */
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
        zoneEntry.saveToDiskV2(indexFile.getFileChannel());
        clearPendingZone();
        cmdBytesSinceLastIndex = 0L;
    }

    private void createNewBlock(String uuid, long gno, int cmdOffset) throws IOException {
        this.blockWriter = new BlockWriter(uuid, gno, cmdOffset, generateBlockName(), byteBuffer);
        this.currentGtidEntry = new IndexEntry(uuid, gno, cmdOffset, blockWriter.getPosition());
    }

    private void flushGtidEntryUnlocked() throws IOException {
        if (!hasPendingGtidEntry()) {
            return;
        }
        currentGtidEntry.setCmdEndOffset(gtidCmdEndOffset);
        currentGtidEntry.saveToDiskV2(blockWriter, indexFile.getFileChannel());
        gtidSetWrapper.compensate(currentGtidEntry);
        closeBlockWriter();
        currentGtidEntry = null;
        cmdBytesSinceLastIndex = 0L;
    }

    private void closeBlockWriter() throws IOException {
        if (blockWriter != null) { blockWriter.close(); blockWriter = null; }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed.compareAndSet(false, true)) return;
        synchronized (writeLock) {
            flushIndexEntryUnlocked();
        }
        super.closeIndexFile();
    }

    public GtidSet getGtidSet() {
        synchronized (writeLock) {
            if (currentGtidEntry != null && blockWriter != null) {
                currentGtidEntry.setSize(blockWriter.getSize());
                gtidSetWrapper.compensate(currentGtidEntry);
            }
            return gtidSetWrapper.getGtidSet();
        }
    }

    // ---------- 恢复 ----------
    private void recoverIndex() throws IOException {
        ControllableFile cmdFile = new DefaultControllableFile(new File(generateCmdFileName()));
        ControllableFile blockFile = new DefaultControllableFile(new File(generateBlockName()));
        try {
            long cmdSize = cmdFile.getFileChannel().size();
            long blockSize = blockFile.getFileChannel().size();

            GtidSet recoverGtidSet = GtidSetWrapper.readGtidSetV2(indexFile.getFileChannel());

            long headerEnd = indexFile.getFileChannel().position();
            long lastValidEndPos = headerEnd;
            long rebuildStart = 0L;

            long pos = headerEnd;
            while (indexFile.getFileChannel().size() - pos >= IndexEntry.SEGMENT_LENGTH_V2) {
                indexFile.getFileChannel().position(pos);
                IndexEntry e = IndexEntry.readFromFileV2(indexFile.getFileChannel());
                if (e == null) break;
                e.setPosition(pos);

                boolean valid;
                if (e.isZone()) {
                    valid = e.getZoneEnd() <= cmdSize;
                } else {
                    valid = e.getBlockEndOffset() != -1
                            && e.getCmdEndOffset() <= cmdSize
                            && e.getBlockEndOffset() <= blockSize;
                }
                if (!valid) break;

                if (e.isZone()) {
                    rebuildStart = Math.max(rebuildStart, e.getZoneEnd());
                } else {
                    if (e.getSize() > 0) recoverGtidSet.compensate(e.getUuid(), e.getStartGno(), e.getEndGno());
                    rebuildStart = Math.max(rebuildStart, e.getCmdEndOffset());
                }
                pos += IndexEntry.SEGMENT_LENGTH_V2;
                lastValidEndPos = pos;
            }

            indexFile.getFileChannel().truncate(lastValidEndPos);
            indexFile.getFileChannel().position(lastValidEndPos);

            this.gtidSetWrapper = new GtidSetWrapper(recoverGtidSet);
            closeBlockWriter();
            this.currentGtidEntry = null;

            log.info("[recoverIndex] {} rebuildStart={}, lastValidEndPos={}, cmdSize={}",
                    super.getFileName(), rebuildStart, lastValidEndPos, cmdSize);
            store.buildIndexFromCmdFile(super.getFileName(), rebuildStart);
        } finally {
            cmdFile.close();
            blockFile.close();
        }
    }

    /** 仅单测使用：读取 index 文件中已落盘的 ZONE 区间。 */
    List<long[]> loadAllZones() throws IOException {
        List<long[]> zones = new ArrayList<>();
        FileChannel ch = indexFile.getFileChannel();
        long headerEnd = GtidSetWrapper.headerSize(ch);
        ch.position(headerEnd);
        while (ch.size() - ch.position() >= IndexEntry.SEGMENT_LENGTH_V2) {
            IndexEntry e = IndexEntry.readFromFileV2(ch);
            if (e == null) break;
            if (e.isZone() && e.getZoneEnd() <= new File(generateCmdFileName()).length()) {
                zones.add(new long[]{e.getZoneStart(), e.getZoneEnd()});
            }
        }
        return zones;
    }
}
