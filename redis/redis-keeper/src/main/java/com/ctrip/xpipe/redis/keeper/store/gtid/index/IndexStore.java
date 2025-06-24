package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

public class IndexStore implements StreamCommandListener, FinishParseDataListener,  Closeable {

    private static final Logger log = LoggerFactory.getLogger(IndexStore.class);

    private IndexWriter indexWriter;

    private StreamCommandReader streamCommandReader;

    private String baseDir;

    private String currentCmdFileName;

    private RedisOpParser opParser;

    private GtidSet startGtidSet;

    private CommandStore commandStore;

    private CommandWriter cmdWriter;

    public IndexStore(String baseDir, RedisOpParser redisOpParser,
                      CommandStore commandStore, CommandWriter cmdWriter) {
        this.baseDir = baseDir;
        this.opParser = redisOpParser;
        this.commandStore = commandStore;
        this.startGtidSet = new GtidSet("");
        this.cmdWriter = cmdWriter;
    }

    public void initialize(CommandWriter cmdWriter) throws IOException {
        this.currentCmdFileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        this.streamCommandReader = new StreamCommandReader(cmdWriter.getFileContext().getChannel().size(), this.opParser);
        this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, startGtidSet, this);
        this.streamCommandReader.addListener(this);
        this.streamCommandReader.addFinishParseDataListener(this);
        this.indexWriter.init();
    }

    public synchronized void initialize(CommandFile commandFile) throws IOException {
        this.currentCmdFileName = commandFile.getFile().getName();
        this.streamCommandReader = new StreamCommandReader(commandFile.getFile().length(), this.opParser);
        this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, startGtidSet, this);
        this.streamCommandReader.addListener(this);
        this.streamCommandReader.addFinishParseDataListener(this);
        this.indexWriter.init();
    }

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

    public synchronized void rotateFileIfNecessary() throws IOException {
        boolean rotate = cmdWriter.rotateFileIfNecessary();
        if(rotate) {
            this.switchCmdFile(cmdWriter);
        }
    }

    public synchronized Pair<Long, GtidSet> locateTailOfCmd() {
        return new Pair<>(cmdWriter.totalLength(), this.getIndexGtidSet());
    }

    @Override
    public void onCommand(String gtid, long offset) {
        String[] parts = gtid.split(":");
        if (parts.length != 2 || parts[0].length() != 40) {
            throw new IllegalArgumentException("Invalid gtid: " + gtid);
        }
        String uuid = parts[0];
        long gno = Long.parseLong(parts[1]);

        try {
            indexWriter.append(uuid, gno, (int)offset);
        } catch (Exception e) {
            log.error("[onCommand fail]", e);
            // todo
        }
    }

    public Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws IOException {
        this.indexWriter.saveIndexEntry();
        try (IndexReader indexReader = new IndexReader(baseDir, currentCmdFileName)) {
            indexReader.init();
            Pair<Long, GtidSet> continuePoint = indexReader.seek(request);
            return continuePoint;
        }
    }

    public synchronized Pair<Long, GtidSet> locateGtidSetWithFallbackToEnd(GtidSet request) throws IOException {
        Pair<Long, GtidSet> continuePoint = locateContinueGtidSet(request);
        if(continuePoint.getKey() == -1) {
            continuePoint = locateTailOfCmd();
        }
        return continuePoint;
    }

    public synchronized GtidSet getIndexGtidSet() {
        return indexWriter.getGtidSet();
    }

    public void buildIndexFromCmdFile(String cmdFileName, long cmdFileOffset) throws IOException {

        this.streamCommandReader = new StreamCommandReader(cmdFileOffset, this.opParser);
        this.streamCommandReader.addListener(this);
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
                controllableFile.setLength((int)controllableFile.size() - (int) remainBytes);
                this.streamCommandReader.relaseRemainBuf();
            }

        } finally {
            // 从cmd 读 写完之后再加入写
            this.streamCommandReader.addFinishParseDataListener(this);
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

    @Override
    public void onFinishParse(ByteBuf byteBuf) throws IOException {
        commandStore.onlyAppendCommand(byteBuf);
    }

}
