package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.google.common.collect.Maps;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class IndexWriter extends AbstractIndex implements Closeable {

    private BlockWriter blockWriter;
    private IndexEntry indexEntry;
    private GtidSetWrapper gtidSetWrapper;
    private IndexStore indexStore;


    public IndexWriter(String baseDir, String fileName, GtidSet gtidSet, IndexStore indexStore) {
        super(baseDir, fileName);
        this.gtidSetWrapper = new GtidSetWrapper(gtidSet);
        this.indexStore = indexStore;
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

        recoverIndex();
        gtidSetWrapper.recover(indexFile.getFileChannel());
    }

    private void recoverIndex() throws IOException {
        ControllableFile cmdFile = null;
        ControllableFile blockFile = null;
        try {
            cmdFile = new DefaultControllableFile(new File(generateCmdFileName()));
            blockFile = new DefaultControllableFile(new File(generateBlockName()));

            long cmdFilePosition = cmdFile.getFileChannel().size();
            long blockFilePosition = blockFile.getFileChannel().size();

            IndexEntry index = this.indexEntry;
            while(index != null && (index.getCmdStartOffset()  > cmdFilePosition ||
                    index.getBlockStartOffset() > blockFilePosition)) {
                // find a IndexEntry which BlockEndOffset and cmd offset is exit.
                index = index.changeToPre();
            }
            if(index != null) {
                this.indexFile.setLength((int)index.getPosition());
                this.blockWriter = null;
                this.indexEntry = null;
                indexStore.buildIndexFromCmdFile(super.getFileName(), index.getCmdStartOffset());
            }
        } finally {
            if(cmdFile != null) {
                cmdFile.close();
            }
        }
    }

    public void append(String uuid, long gno, int commandOffset) throws IOException {

        if(blockWriter == null) {
            this.createNewBlock(uuid, gno, 0);
        } else {
            if(needChangeBlock(uuid, gno)) {
                changeBlock(uuid, gno, this.blockWriter.getCmdOffset());
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


    private void createNewBlock(String uuid, long gno, int commandOffset) throws IOException {
        this.blockWriter = new BlockWriter(uuid, gno, commandOffset, generateBlockName());
        this.indexEntry = new IndexEntry(uuid, gno, commandOffset, this.blockWriter.getPosition());
        saveIndexEntry();
    }

    private void changeBlock(String uuid, long gno, int commandOffset) throws IOException {
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
