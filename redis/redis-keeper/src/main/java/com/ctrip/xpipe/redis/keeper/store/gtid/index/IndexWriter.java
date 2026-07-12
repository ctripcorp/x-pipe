package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX;

public class IndexWriter implements Closeable {

    private final DefaultIndexStore store;
    private final AsyncCommandStore cmdStore;
    private final AsyncFileSystem fs;

    private BlockEntry currentBlock;
    private IndexEntry indexEntry;
    private GtidSetWrapper gtidSetWrapper;
    private AsyncFile indexFile;
    private AsyncFile blockFile;
    private volatile boolean closed;

    public IndexWriter(GtidSet gtidSet, DefaultIndexStore store) {
        this.store = store;
        this.cmdStore = store.getAsyncCommandStore();
        this.fs = cmdStore.getAsyncFileSystem();
        this.gtidSetWrapper = new GtidSetWrapper(gtidSet);
    }

    public void init(AsyncFile indexFile, AsyncFile blockFile) throws IOException {
        this.indexFile = indexFile;
        this.blockFile = blockFile;
        ensureHeaderIfEmpty();
    }

    void ensureHeaderIfEmpty() throws IOException {
        if (indexFile == null) {
            return;
        }
        if (AsyncFileSystemHelper.await(fs.size(indexFile), "size index v1") == 0) {
            gtidSetWrapper.saveGtidSet(fs, indexFile);
        }
    }

    void recoverIndex(AsyncFile indexFile, AsyncFile blockFile) throws IOException {
        AsyncFileSystemHelper.await(fs.position(indexFile, 0), "position index v1 to start for recover");
        AsyncFileSystemHelper.await(fs.position(blockFile, 0), "position block v1 to start for recover");

        String cmdPrefix = cmdStore.getCommandFileNamePrefix();
        if (!GtidSetWrapper.isV1HeaderComplete(fs, indexFile)) {
            this.gtidSetWrapper = new GtidSetWrapper(new GtidSet(""));
            this.indexEntry = null;
            store.buildIndexFromCmdFile(0, INDEX + cmdPrefix, BLOCK + cmdPrefix, 0, 0);
            return;
        }
        long cmdSize = cmdStore.currentSegmentSize();
        long blockSize = AsyncFileSystemHelper.await(fs.size(blockFile), "size block v1");
        long headerEnd = readV1HeaderEnd(indexFile);

        this.indexEntry = gtidSetWrapper.recover(fs, indexFile);
        IndexEntry index = this.indexEntry;
        while (index != null && (index.getCmdStartOffset() > cmdSize || index.getBlockStartOffset() > blockSize)) {
            index = readPreIndexEntry(index, indexFile, headerEnd);
        }
        if (index != null) {
            long indexSize = index.getPosition() + IndexEntry.SEGMENT_LENGTH;
            long blockTruncate = index.getBlockEndOffset() >= 0 ? index.getBlockEndOffset() : blockSize;
            currentBlock = null;
            this.indexEntry = null;
            store.buildIndexFromCmdFile(index.getCmdStartOffset(), INDEX + cmdPrefix, BLOCK + cmdPrefix, indexSize, blockTruncate);
        } else {
            this.indexEntry = null;
            store.buildIndexFromCmdFile(0, INDEX + cmdPrefix, BLOCK + cmdPrefix, headerEnd, 0);
        }
    }

    private long readV1HeaderEnd(AsyncFile indexFile) throws IOException {
        ByteBuf lenBuf = AsyncFileSystemHelper.await(fs.read(indexFile, Long.BYTES, 0), "read v1 header len");
        try {
            return Long.BYTES + lenBuf.readLong();
        } finally {
            lenBuf.release();
        }
    }

    private IndexEntry readPreIndexEntry(IndexEntry currentEntry, AsyncFile indexFile, long headerEnd) throws IOException {
        long preIndex = currentEntry.getPosition() - IndexEntry.SEGMENT_LENGTH;
        if (preIndex < headerEnd) {
            return null;
        }
        ByteBuf buf = AsyncFileSystemHelper.await(fs.read(indexFile, IndexEntry.SEGMENT_LENGTH, preIndex),
                "read previous index entry");
        try {
            IndexEntry result = IndexEntry.fromBuffer(buf);
            if (result != null) {
                result.setPosition(preIndex);
            }
            return result;
        } finally {
            buf.release();
        }
    }

    public void append(String uuid, long gno, int commandOffset) throws IOException {
        if (currentBlock == null) {
            createNewBlock(uuid, gno, commandOffset);
        } else if (currentBlock.needChangeBlock(uuid, gno)) {
            changeBlock(uuid, gno, commandOffset);
        }
        currentBlock.append(uuid, gno, commandOffset);
    }

    private void finishBlock() throws IOException {
        saveIndexEntry();
        gtidSetWrapper.compensate(indexEntry);
        currentBlock = null;
    }

    private void createNewBlock(String uuid, long gno, int commandOffset) throws IOException {
        this.currentBlock = new BlockEntry(uuid, gno, commandOffset);
        long blockStart = AsyncFileSystemHelper.await(fs.size(blockFile), "size block v1");
        this.indexEntry = new IndexEntry(uuid, gno, commandOffset, blockStart);
        saveIndexEntry();
    }

    private void changeBlock(String uuid, long gno, int commandOffset) throws IOException {
        finishBlock();
        createNewBlock(uuid, gno, commandOffset);
    }

    public void finish() throws IOException {
        finishBlock();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        saveIndexEntry();
        currentBlock = null;
    }

    public GtidSet getGtidSet() {
        gtidSetWrapper.compensate(indexEntry, currentBlock);
        return gtidSetWrapper.getGtidSet();
    }

    public void saveIndexEntry() throws IOException {
        if (closed || indexEntry == null) {
            return;
        }
        indexEntry.saveToDisk(fs, indexFile, currentBlock, blockFile);
    }

}
