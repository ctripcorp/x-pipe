package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import com.google.common.collect.Maps;

import java.io.Closeable;
import java.io.IOException;

public class IndexWriter extends AbstractIndex implements Closeable {

    private BlockWriter blockWriter;
    private IndexEntry indexEntry;
    private GtidSetWrapper gtidSetWrapper;


    public IndexWriter(String baseDir, String fileName, GtidSet gtidSet) {
        super(baseDir, fileName);
        this.gtidSetWrapper = new GtidSetWrapper(gtidSet);
    }


    public void init() throws IOException {

        super.initIndexFile();

        if(indexFile.getFileChannel().size() == 0) {
            initForCreateNew();
        } else {
            initForContinue();
        }

    }

    private void initForCreateNew() throws IOException {
        if(gtidSetWrapper == null) {
            gtidSetWrapper = new GtidSetWrapper(new GtidSet(Maps.newLinkedHashMap()));
        }
        gtidSetWrapper.saveGtidSet(indexFile.getFileChannel());

    }

    private void initForContinue() throws IOException {

        gtidSetWrapper = new GtidSetWrapper(new GtidSet(Maps.newLinkedHashMap()));
        this.indexEntry = gtidSetWrapper.recover(indexFile.getFileChannel());

        if(this.indexEntry != null) {
            // recover block writer
            this.blockWriter = new BlockWriter(this.indexEntry.getUuid(),
                    this.indexEntry.getStartGno() - 1, (int) indexEntry.getCmdStartOffset(), generateBlockName());
            this.blockWriter.recover(indexEntry.getBlockStartOffset(), this.indexEntry.getStartGno());
        }

    }

    public void append(String uuid, long gno, int commandOffset) throws Exception {

        if(blockWriter == null) {
            this.createNewBlock(uuid, gno, commandOffset);
        } else {
            if(needChangeBlock(uuid, gno)) {
                changeBlock(uuid, gno, commandOffset);
            }
        }
        this.blockWriter.append(uuid, gno, commandOffset);
    }


    private boolean needChangeBlock(String uuid, long gno) {
        return blockWriter.needChangeBlock(uuid, gno);
    }


    private void finishBlock() throws IOException {
        if(blockWriter != null) {
            blockWriter.close();
        }
        saveIndexEntry();
        gtidSetWrapper.compensate(indexEntry);
    }


    private void createNewBlock(String uuid, long gno, int commandOffset) throws Exception {
        this.blockWriter = new BlockWriter(uuid, gno, commandOffset, generateBlockName());
        this.indexEntry = new IndexEntry(uuid, gno, commandOffset, this.blockWriter.getPosition());
        saveIndexEntry();
    }

    private void changeBlock(String uuid, long gno, int commandOffset) throws Exception {
        this.finishBlock();
        this.createNewBlock(uuid, gno, commandOffset);
    }

    public void finish() throws IOException {
        this.finishBlock();
    }

    @Override
    public void close() throws IOException {
        saveIndexEntry();
        super.closeIndexFile();
        if(blockWriter != null) {
            this.blockWriter.finishWriter();
            this.blockWriter.close();
        }
    }

    public GtidSet getGtidSet() {
        gtidSetWrapper.compensate(indexEntry, blockWriter);
        return gtidSetWrapper.getGtidSet();
    }

    public void saveIndexEntry() throws IOException {
        if(indexEntry != null) {
            indexEntry.saveToDisk(blockWriter, indexFile.getFileChannel());
        }
    }

}
