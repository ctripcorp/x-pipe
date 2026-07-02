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
    static final int FLUSH_THRESHOLD = 8192;

    private BlockWriter blockWriter;
    private IndexEntry currentGtidEntry;
    private GtidSetWrapper gtidSetWrapper;
    private DefaultIndexStore store;
    private ByteBuffer byteBuffer;

    private long zoneStart = -1L, zoneEnd = 0L;
    private int zoneCmdCount = 0;
    private int lastCommandLength = 0;

    public IndexWriterV2(String baseDir, String cmdFileName, GtidSet gtidSet, DefaultIndexStore store) {
        super(baseDir, cmdFileName);
        this.gtidSetWrapper = new GtidSetWrapper(gtidSet);
        this.store = store;
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

    // GTID 写入
    public void append(String uuid, long gno, int commandOffset) throws IOException {
        flushZone();
        if (blockWriter == null || needChangeBlock(uuid, gno)) {
            if (blockWriter != null) finishBlock();
            createNewBlock(uuid, gno, commandOffset);
        }
        blockWriter.append(uuid, gno, commandOffset);
    }

    public void setLastCommandLength(int length) { this.lastCommandLength = length; }

    // 非 GTID 写入
    public void appendNonGtid(long offset, int length) throws IOException {
        if (zoneStart == -1L) zoneStart = offset;
        zoneEnd = offset + length;
        if (++zoneCmdCount >= FLUSH_THRESHOLD) flushZone();
    }

    public void flushZone() throws IOException {
        if (isClosed.get() || zoneStart == -1L) return;
        IndexEntry zoneEntry = IndexEntry.zone(zoneStart, zoneEnd, zoneCmdCount);
        zoneEntry.saveToDiskV2(indexFile.getFileChannel());
        zoneStart = -1L; zoneEnd = 0L; zoneCmdCount = 0;
    }

    private void createNewBlock(String uuid, long gno, int cmdOffset) throws IOException {
        this.blockWriter = new BlockWriter(uuid, gno, cmdOffset, generateBlockName(), byteBuffer);
        this.currentGtidEntry = new IndexEntry(uuid, gno, cmdOffset, blockWriter.getPosition());
    }

    private void finishBlock() throws IOException {
        saveIndexEntryV2();
    }

    public void saveIndexEntryV2() throws IOException {
        if(isClosed.get()) {
            return;
        }
        if(currentGtidEntry != null) {
            currentGtidEntry.setLastCommandLength(lastCommandLength);
            currentGtidEntry.saveToDiskV2(blockWriter, indexFile.getFileChannel());
            gtidSetWrapper.compensate(currentGtidEntry);
            closeBlockWriter();
            currentGtidEntry = null;
        }
    }

    private boolean needChangeBlock(String uuid, long gno) {
        return blockWriter != null && blockWriter.needChangeBlock(uuid, gno);
    }

    private void closeBlockWriter() throws IOException {
        if (blockWriter != null) { blockWriter.close(); blockWriter = null; }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed.compareAndSet(false, true)) return;
        flushZone();
        finishBlock();
        super.closeIndexFile();
    }

    public GtidSet getGtidSet() {
        if (currentGtidEntry != null && blockWriter != null) {
            currentGtidEntry.setSize(blockWriter.getSize());
            gtidSetWrapper.compensate(currentGtidEntry);
        }
        return gtidSetWrapper.getGtidSet();
    }

    // ---------- 恢复 ----------
    private void recoverIndex() throws IOException {
        ControllableFile cmdFile = new DefaultControllableFile(new File(generateCmdFileName()));
        ControllableFile blockFile = new DefaultControllableFile(new File(generateBlockName()));
        try {
            long cmdSize = cmdFile.getFileChannel().size();
            long blockSize = blockFile.getFileChannel().size();

            GtidSet recoverGtidSet = GtidSetWrapper.readGtidSetV2(indexFile.getFileChannel());

            List<IndexEntry> allEntries = new ArrayList<>();
            List<long[]> zones = new ArrayList<>();
            IndexEntry lastCompleteGtid = null;

            long pos = indexFile.getFileChannel().position();
            while (indexFile.getFileChannel().size() - pos >= IndexEntry.SEGMENT_LENGTH_V2) {
                indexFile.getFileChannel().position(pos);
                IndexEntry e = IndexEntry.readFromFileV2(indexFile.getFileChannel());
                if (e == null) break;
                e.setPosition(pos);
                allEntries.add(e);
                pos += IndexEntry.SEGMENT_LENGTH_V2;

                if (e.isZone()) {
                    if (e.getZoneEnd() <= cmdSize) zones.add(new long[]{e.getZoneStart(), e.getZoneEnd()});
                } else {
                    if (e.getSize() > 0) recoverGtidSet.compensate(e.getUuid(), e.getStartGno(), e.getEndGno());
                    if (e.getBlockEndOffset() != -1 &&
                            e.getCmdStartOffset() <= cmdSize &&
                            e.getBlockEndOffset() <= blockSize) {
                        lastCompleteGtid = e;
                    }
                }
            }

            long rebuildStart;
            if (lastCompleteGtid != null) {
                long lastDiff = new BlockReader(lastCompleteGtid.getBlockStartOffset(),
                        lastCompleteGtid.getBlockEndOffset(), new File(generateBlockName()))
                        .seek(lastCompleteGtid.getSize() - 1);
                long lastCmdStart = lastCompleteGtid.getCmdStartOffset() + lastDiff;
                long cmdEndOffset = lastCmdStart + lastCompleteGtid.getLastCommandLength();

                long truncPos = lastCompleteGtid.getPosition() + IndexEntry.SEGMENT_LENGTH_V2;
                indexFile.getFileChannel().truncate(truncPos);
                indexFile.getFileChannel().position(truncPos);
                rebuildStart = cmdEndOffset;
            } else {
                long headerEnd = GtidSetWrapper.headerSize(indexFile.getFileChannel());
                indexFile.getFileChannel().truncate(headerEnd);
                indexFile.getFileChannel().position(headerEnd);
                rebuildStart = 0L;
            }

            this.gtidSetWrapper = new GtidSetWrapper(recoverGtidSet);
            closeBlockWriter();
            this.currentGtidEntry = null;

            store.buildIndexFromCmdFileWithZones(super.getFileName(), rebuildStart, zones);
        } finally {
            cmdFile.close();
            blockFile.close();
        }
    }

    public List<long[]> loadAllZones() throws IOException {
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
