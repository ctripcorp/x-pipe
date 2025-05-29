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

    private static final Logger log = LoggerFactory.getLogger(IndexStore.class);

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

        for (int i = indexItemList.size() - 1; i >= 0; i--) {
            IndexEntry indexEntry = indexItemList.get(i);
            if(indexEntry.getBlockEndOffset() == -1) {
                // 每次查之前需要保存一下，如果遇到没有保存是-1
                continue;

            }
            GtidSet.UUIDSet uuidSet = request.getUUIDSet(indexEntry.getUuid());

            if(uuidSet != null) {
                long gno = uuidSet.getLastGno();
                if(gno > indexEntry.getEndGno()) {
                    // 读block 最后一个数字
                    try(BlockReader blockReader = new BlockReader(indexEntry.getBlockStartOffset(), indexEntry.getBlockEndOffset(),
                            new File(generateBlockName()))) {
                        long index  = indexEntry.getEndGno() - indexEntry.getStartGno();
                        return new Pair<>(blockReader.seek((int)index) + indexEntry.getCmdStartOffset(), calculateGtidSet(i, indexEntry.getEndGno()));
                    }
                } else if (gno >= indexEntry.getStartGno() && gno <= indexEntry.getEndGno()) {
                    // 读block 获取
                    try(BlockReader blockReader = new BlockReader(indexEntry.getBlockStartOffset(), indexEntry.getBlockEndOffset(),
                            new File(generateBlockName()))) {
                        long index = gno - indexEntry.getStartGno();
                        return new Pair<>(blockReader.seek((int) index) + indexEntry.getCmdStartOffset(), calculateGtidSet(i, gno));
                    }
                }
            }
        }
        return new Pair<>(-1l, startGtidSet);
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

    @Override
    public void close() throws IOException {
        super.closeIndexFile();
    }
}
