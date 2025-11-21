package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class IndexReader extends AbstractIndex implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DefaultIndexStore.class);

    private List<IndexEntry> indexItemList;

    private GtidSet startGtidSet;

    public IndexReader(String baseDir, String indexFile) {
        super(baseDir, indexFile);
    }

    public GtidSet getStartGtidSet() {
        return startGtidSet;
    }



    public void init() throws IOException {

        indexItemList = new ArrayList<>();
        super.initIndexFile();

        if(indexFile.getFileChannel().size() == 0) {
            log.warn("[IndexReader], file: {} length is 0", indexFile);
            startGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
            return;
        }
        // skip gtid set
        startGtidSet =  GtidSetWrapper.readGtidSet(indexFile.getFileChannel());
        IndexEntry item = IndexEntry.readFromFile(indexFile.getFileChannel());
        while (item != null) {
            indexItemList.add(item);
            item = IndexEntry.readFromFile(indexFile.getFileChannel());
        }
    }

    // ignore only call by test
    public long seek(String uuid, long gno) throws IOException {
        int index = -1;
        for(int i = indexItemList.size() - 1; i >= 0; i--) {
            IndexEntry item = indexItemList.get(i);
            if(!StringUtil.trimEquals(item.getUuid(), uuid)) {
                continue;
            }
            if(gno >= item.getStartGno() && gno <= item.getEndGno()) {
                index = i;
                break;
            };
        }

        // todo fix -1
        IndexEntry item = indexItemList.get(index);
        if(gno == item.getStartGno()) {
            return item.getCmdStartOffset();
        }

        int arrayIndex = (int) gno - (int) item.getStartGno();

        try (BlockReader blockReader = new BlockReader(item.getBlockStartOffset(), item.getBlockEndOffset(),
                new File(generateBlockName()))
        ){
            return blockReader.seek(arrayIndex) + item.getCmdStartOffset();
        }

    }

    public Pair<Long, GtidSet> seek(GtidSet request) throws IOException {

        long offset = -1L;
        GtidSet gtidSet = null;
        boolean changeFileSuccess = true;
        boolean finish = false;
        while (changeFileSuccess) {
            for (int i = indexItemList.size() - 1; i >= 0; i--) {
                IndexEntry indexEntry = indexItemList.get(i);
                if(indexEntry.getBlockEndOffset() == -1) {
                    // 每次查之前需要保存一下，如果遇到没有保存是-1
                    continue;
                }
                GtidSet.UUIDSet uuidSet = request.getUUIDSet(indexEntry.getUuid());
                long nexGno = uuidSet != null ? uuidSet.getLastGno() + 1 : GtidSet.GTID_GNO_INITIAL;

                if(nexGno > indexEntry.getEndGno()) {
                    finish = true;
                    if(gtidSet == null) {
                        gtidSet = calculateGtidSet(i + 1);
                    }
                    break;
                } else if (nexGno >= indexEntry.getStartGno() && nexGno <= indexEntry.getEndGno()) {
                    // 读block 获取
                    try(BlockReader blockReader = new BlockReader(indexEntry.getBlockStartOffset(), indexEntry.getBlockEndOffset(),
                            new File(generateBlockName()))) {
                        long index = nexGno - indexEntry.getStartGno();
                        offset = blockReader.seek((int) index) + indexEntry.getCmdStartOffset() + getStartOffset();
                        gtidSet = calculateGtidSet(i, nexGno - 1);
                        finish = true;
                        break;
                    }
                } else if(nexGno < indexEntry.getStartGno()) {
                    offset = indexEntry.getCmdStartOffset() + getStartOffset();
                    gtidSet = calculateGtidSet(i);
                }
            }
            // find in pre index;
            if(finish) {
                break;
            }
            changeFileSuccess = this.changeToPre();
        }

        if(gtidSet == null) {
            gtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
        }
        return new Pair<>(offset, gtidSet);
    }

    public Pair<Long, GtidSet> getFirstPoint() throws IOException {
        if(noIndex()) {
            return new Pair<>(0L, startGtidSet);
        }
        IndexEntry indexEntry = indexItemList.get(0);
        try(BlockReader blockReader = new BlockReader(indexEntry.getBlockStartOffset(), indexEntry.getBlockEndOffset(),
                new File(generateBlockName()))) {
            long index  = 0;
            return new Pair<>(blockReader.seek((int)index) + indexEntry.getCmdStartOffset(),
                    calculateGtidSet(0, indexEntry.getStartGno()));
        }
    }

    public boolean noIndex() {
        return indexItemList.size() == 0;
    }

    //caculate continue gtid set
    private GtidSet calculateGtidSet(int index, long gno) {
        GtidSet startGtid = new GtidSet(getStartGtidSet().toString());
        GtidSetWrapper gtidSetWrapper = new GtidSetWrapper(startGtid);
        for(int i = 0; i < index; i++) {
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
        for(int i = 0; i < index; i++) {
            IndexEntry indexEntry = indexItemList.get(i);
            gtidSetWrapper.compensate(indexEntry.getUuid(), indexEntry.getStartGno(), indexEntry.getEndGno());
        }
        return gtidSetWrapper.getGtidSet();
    }

    @Override
    public void close() throws IOException {
        super.closeIndexFile();
    }

    public GtidSet getAllGtidSet() {
        int len = indexItemList.size();
        return calculateGtidSet(len);
    }

    public static IndexReader getLastIndexReader(String baseDir) {
        File lastIndexFile = findFloorIndexFileByOffset(baseDir, -1);
        if(lastIndexFile == null) {
            return null;
        }
        String fileName = lastIndexFile.getName().replace(INDEX, "");
        IndexReader indexReader = new IndexReader(baseDir, fileName);
        return indexReader;
    }

    public static IndexReader getFirstIndexReader(String baseDir) {
        File firstIndexFile = findFirstIndexFileByOffset(baseDir);
        if(firstIndexFile == null) {
            return null;
        }
        String fileName = firstIndexFile.getName().replace(INDEX, "");
        IndexReader indexReader = new IndexReader(baseDir, fileName);
        return indexReader;
    }

    /**
     * Calculate the cmdStartOffset for a specific GNO within an IndexEntry.
     */
    private long calculateOffsetForGno(IndexEntry item, long gno) throws IOException {
        if(gno == item.getStartGno()) {
            return item.getCmdStartOffset();
        }
        
        int arrayIndex = (int) (gno - item.getStartGno());
        try (BlockReader blockReader = new BlockReader(item.getBlockStartOffset(), item.getBlockEndOffset(),
                new File(generateBlockName()))
        ){
            return blockReader.seek(arrayIndex) + item.getCmdStartOffset();
        }
    }

    /**
     * Find all matching ranges in [begGno, endGno] for the given uuid in current index file.
     * Returns a list of pairs, each pair represents a range (startOffset, endOffset) in cmdStartOffset.
     * endOffset is the start offset of the next command, or null if it's the last command in the file.
     */
    public List<Pair<Long, Long>> findMatchingRanges(String uuid, long begGno, long endGno) throws IOException {
        List<Pair<Long, Long>> ranges = new ArrayList<>();
        
        for(int i = 0; i < indexItemList.size(); i++) {
            IndexEntry item = indexItemList.get(i);
            if(!StringUtil.trimEquals(item.getUuid(), uuid)) {
                continue;
            }
            
            long entryStartGno = item.getStartGno();
            long entryEndGno = item.getEndGno();
            
            // Check if ranges overlap: [begGno, endGno] and [entryStartGno, entryEndGno]
            if(entryEndGno < begGno || entryStartGno > endGno) {
                continue; // No overlap
            }
            
            // Skip if entry is not fully written
            if(item.getBlockEndOffset() == -1) {
                continue;
            }
            
            // Calculate the actual range within the entry
            long rangeStartGno = Math.max(entryStartGno, begGno);
            long rangeEndGno = Math.min(entryEndGno, endGno);
            
            // Calculate start offset
            long startOffset = calculateOffsetForGno(item, rangeStartGno);
            
            // Calculate end offset (next command's start, or null if last)
            Long endOffset;
            if(rangeEndGno < item.getEndGno()) {
                // Not the last gno in this entry, get next gno's start offset
                endOffset = calculateOffsetForGno(item, rangeEndGno + 1);
            } else if (i + 1 < indexItemList.size()) {
                IndexEntry nextEntry = indexItemList.get(i + 1);
                endOffset = nextEntry.getCmdStartOffset();
            } else {
                // If no next entry found, endOffset remains null (will use file end)
                endOffset = null;
            }
            
            ranges.add(new Pair<>(startOffset, endOffset));
        }
        
        return ranges;
    }
}
