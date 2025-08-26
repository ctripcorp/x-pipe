package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.core.store.GtidCmdFilter;
import com.ctrip.xpipe.redis.core.store.IndexStore;
import com.ctrip.xpipe.redis.keeper.exception.replication.LostGtidsetBacklogConflictException;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

public class DefaultIndexStore implements IndexStore {

    private static final Logger log = LoggerFactory.getLogger(DefaultIndexStore.class);

    private IndexWriter indexWriter;

    private StreamCommandReader streamCommandReader;

    private String baseDir;

    private String currentCmdFileName;

    private RedisOpParser opParser;

    private GtidSet startGtidSet;

    private CommandStore commandStore;

    private GtidCmdFilter gtidCmdFilter;

    private boolean writerCmdEnabled;

    private CommandStore parentCommandStore;

    public DefaultIndexStore(String baseDir, RedisOpParser redisOpParser,
                             CommandStore commandStore, CommandStore cmdStore, GtidCmdFilter gtidCmdFilter) {
        this.baseDir = baseDir;
        this.opParser = redisOpParser;
        this.commandStore = commandStore;
        this.startGtidSet = new GtidSet("");
        this.parentCommandStore = commandStore;
        this.gtidCmdFilter = gtidCmdFilter;
        this.writerCmdEnabled = true;
    }

    @Override
    public void initialize(CommandWriter cmdWriter) throws IOException {
        this.currentCmdFileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        this.streamCommandReader = new StreamCommandReader(this, cmdWriter.getFileContext().getChannel().size(), this.opParser);
        this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, startGtidSet, this);
        this.indexWriter.init();
    }

    @Override
    public synchronized void write(ByteBuf byteBuf) throws IOException {
        streamCommandReader.doRead(byteBuf);
    }

    public void switchCmdFile(CommandWriter cmdWriter) throws IOException {
        String fileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        switchCmdFile(fileName);
    }

    public synchronized void switchCmdFile(String cmdFileName) throws IOException {
        GtidSet continueGtidSet = this.indexWriter.getGtidSet();
        this.currentCmdFileName = cmdFileName;
        this.indexWriter.close();
        this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, continueGtidSet, this);
        this.indexWriter.init();
        this.streamCommandReader.resetOffset();
        log.info("[switchCmdFile] index_store switch to {}", currentCmdFileName);
    }

    @Override
    public synchronized void rotateFileIfNecessary() throws IOException {
        boolean rotate = parentCommandStore.getCommandWriter().rotateFileIfNecessary();
        if(rotate) {
            this.switchCmdFile(parentCommandStore.getCommandWriter());
        }
    }

    @Override
    public synchronized Pair<Long, GtidSet> locateTailOfCmd() {
        return new Pair<>(parentCommandStore.getCommandWriter().totalLength(), this.getIndexGtidSet());
    }

    public boolean onCommand(String gtid, long offset) throws IOException {
        String[] parts = gtid.split(":");
        if (parts.length != 2 || parts[0].length() != 40) {
            throw new IllegalArgumentException("Invalid gtid: " + gtid);
        }
        String uuid = parts[0];
        long gno = Long.parseLong(parts[1]);
        if(gtidCmdFilter.gtidSetContains(uuid, gno)) {
            log.info("[onCommand] gtid command {} in lost, ignored", gtid);
            return false;
        }
        indexWriter.append(uuid, gno, (int)offset);
        return true;
    }

    @Override
    public Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws IOException {
        this.indexWriter.saveIndexEntry();
        try (IndexReader indexReader = new IndexReader(baseDir, currentCmdFileName)) {
            indexReader.init();
            Pair<Long, GtidSet> continuePoint = indexReader.seek(request);
            return continuePoint;
        }
    }

    @Override
    public synchronized Pair<Long, GtidSet> locateGtidSetWithFallbackToEnd(GtidSet request) throws IOException {
        Pair<Long, GtidSet> continuePoint = locateContinueGtidSet(request);
        if(continuePoint.getKey() == -1) {
            log.info("[locateGtidSetWithFallbackToEnd] not found next, return tail of cmd, request:{}", request);
            continuePoint = locateTailOfCmd();
        }
        log.info("backlog gtid set: {}, request gtid set {}, continue gtid set {}", getIndexGtidSet(),
                request, continuePoint.getValue());
        return continuePoint;
    }

    @Override
    public synchronized GtidSet getIndexGtidSet() {
        return indexWriter.getGtidSet();
    }

    @Override
    public synchronized boolean increaseLost(GtidSet lost, IOSupplier<Boolean> supplier) throws IOException {
        GtidSet backlogGtidSet = getIndexGtidSet();
        GtidSet intersection = backlogGtidSet.retainAll(lost);
        if(intersection.itemCnt() > 0) {
            throw new LostGtidsetBacklogConflictException("increase lost conflict with backlog");
        }
        return supplier.get();
    }

    public void buildIndexFromCmdFile(String cmdFileName, long cmdFileOffset) throws IOException {
        this.streamCommandReader = new StreamCommandReader(this, cmdFileOffset, this.opParser);
        this.disableWriterCmd();
        ControllableFile controllableFile = null;
        try {
            File f = new File(Paths.get(baseDir, cmdFileName).toString());
            controllableFile = new DefaultControllableFile(f);
            controllableFile.getFileChannel().position(cmdFileOffset);
            while(controllableFile.getFileChannel().position() < controllableFile.getFileChannel().size()) {
                int size = (int)Math.min(1024*8, controllableFile.getFileChannel().size() - controllableFile.getFileChannel().position());
                ByteBuffer buffer = ByteBuffer.allocate(size);
                controllableFile.getFileChannel().read(buffer);
                buffer.flip();
                ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
                this.write(byteBuf);
            }
            long remainBytes = this.streamCommandReader.getRemainLength();
            if(remainBytes > 0) {
                EventMonitor.DEFAULT.logAlertEvent("TRUNCATE_CMD_FILE");
                controllableFile.setLength((int)controllableFile.size() - (int) remainBytes);
                this.streamCommandReader.relaseRemainBuf();
            }

        } finally {
            // 从cmd 读 写完之后再加入写
            this.enableWriterCmd();
            if(controllableFile != null) {
                controllableFile.close();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if(this.streamCommandReader != null) {
            this.streamCommandReader.relaseRemainBuf();
        }
        if(this.indexWriter != null) {
            log.debug("[doClose] close index writer {}", indexWriter.getFileName());
            this.indexWriter.close();
        }
    }

    @Override
    public void closeWithDeleteIndexFiles() throws IOException {
        this.close();
        deleteAllIndexFile();
    }

    public void deleteAllIndexFile() {
        File directory = new File(baseDir);

        log.info("[deleteAllIndexFile] {}", baseDir);
        if (!directory.exists() || !directory.isDirectory()) {
            return;
        }
        File[] files = directory.listFiles((dir, name) -> name.startsWith(AbstractIndex.INDEX) || name.startsWith(AbstractIndex.BLOCK));
        if (files == null) {
            return;
        }
        for (File file : files) {
            file.delete();
        }
    }

    public void onFinishParse(ByteBuf byteBuf) throws IOException {
        if(writerCmdEnabled) {
            commandStore.onlyAppendCommand(byteBuf);
        }
    }

    private void disableWriterCmd() {
        this.writerCmdEnabled = false;
    }

    private void enableWriterCmd() {
        this.writerCmdEnabled = true;
    }

}
