package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;

import java.io.File;
import java.io.IOException;

/**
 * @author TB
 * @date 2026/6/29 20:43
 */

public class IndexReaderV2 extends IndexReader {

    public IndexReaderV2(String baseDir, String indexFile) {
        super(baseDir, indexFile);
    }

    @Override
    protected String getIndexPrefix() { return INDEX_V2; }
    @Override
    protected String getBlockPrefix() { return BLOCK_V2; }

    @Override
    public void init() throws IOException {
        indexItemList = new java.util.ArrayList<>();
        super.initIndexFile();
        if (indexFile.getFileChannel().size() == 0) {
            startGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
            return;
        }
        startGtidSet= GtidSetWrapper.readGtidSetV2(indexFile.getFileChannel());
        IndexEntry item = IndexEntry.readFromFileV2(indexFile.getFileChannel());
        while (item != null) {
            if (!item.isZone()) indexItemList.add(item);
            item = IndexEntry.readFromFileV2(indexFile.getFileChannel());
        }
    }

    public static IndexReader getLastIndexReader(String baseDir) {
        File lastIndexFile = findFloorIndexFileByOffset(baseDir, -1,INDEX_V2);
        if(lastIndexFile == null) {
            return null;
        }
        String fileName = lastIndexFile.getName().replace(INDEX_V2, "");
        return new IndexReaderV2(baseDir, fileName);
    }

    public static IndexReader getFirstIndexReader(String baseDir) {
        File firstIndexFile = findFirstIndexFileByOffset(baseDir,INDEX_V2);
        if(firstIndexFile == null) {
            return null;
        }
        String fileName = firstIndexFile.getName().replace(INDEX_V2, "");
        return new IndexReaderV2(baseDir, fileName);
    }
}
