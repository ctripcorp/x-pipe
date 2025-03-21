package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.utils.StringUtil;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class IndexReader extends AbstractIndex implements Closeable {

    private List<IndexEntry> indexItemList;

    public IndexReader(String baseDir, String indexFile) {
        super(baseDir, indexFile);
    }


    public void init() throws IOException {
        super.initIndexFile();
        // skip gtid set
        indexItemList = new ArrayList<>();
        GtidSetWrapper.skipGtidSet(indexFile.getFileChannel());
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

    public long seek(GtidSet request) throws IOException {
        // 如果没有匹配到 返回是第一个
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
                        return blockReader.seek((int)index) + + indexEntry.getCmdStartOffset();
                    }
                } else if (gno >= indexEntry.getStartGno() && gno <= indexEntry.getEndGno()) {
                    // 读block 获取
                    try(BlockReader blockReader = new BlockReader(indexEntry.getBlockStartOffset(), indexEntry.getBlockEndOffset(),
                            new File(generateBlockName()))) {
                        long index = gno - indexEntry.getStartGno();
                        return blockReader.seek((int) index) + + indexEntry.getCmdStartOffset();
                    }
                }
            }
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        super.closeIndexFile();
    }
}
