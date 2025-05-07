package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class IndexStore implements StreamCommandListener, Closeable {

    private IndexWriter indexWriter;

    private StreamCommandReader streamCommandReader;

    private String baseDir;

    private String currentCmdFileName;

    private RedisOpParser opParser;

    private GtidSet startGtidSet;

    public IndexStore(String baseDir, RedisOpParser redisOpParser) {
        this.baseDir = baseDir;
        this.opParser = redisOpParser;
        this.startGtidSet = new GtidSet("");
    }


    public void initialize(CommandWriter cmdWriter) throws IOException {
        this.currentCmdFileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        this.streamCommandReader = new StreamCommandReader(cmdWriter.getFileContext().getChannel().size(), this.opParser);
        this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, startGtidSet, this);
        this.streamCommandReader.addListener(this);
        this.indexWriter.init();
    }

    public void write(ByteBuf byteBuf) {
        streamCommandReader.doRead(byteBuf);

    }

    public void switchCmdFile(CommandWriter cmdWriter) throws IOException {
        String fileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        switchCmdFile(fileName);
    }

    public void switchCmdFile(String cmdFileName) throws IOException {
        this.indexWriter.finish();
        GtidSet continueGtidSet = this.indexWriter.getGtidSet();
        this.currentCmdFileName = cmdFileName;
        this.indexWriter.close();
        this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, continueGtidSet, this);
        this.indexWriter.init();
        this.streamCommandReader.resetOffset();
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
            // todo
        }
    }

    public Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws Exception {
        this.indexWriter.saveIndexEntry();
        long offset = -1;
        long baseOffset = 0l;
        GtidSet continueGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
        try (IndexReader indexReader = new IndexReader(baseDir, currentCmdFileName)) {
            indexReader.init();
            Pair<Long, GtidSet> continuePoint = indexReader.seek(request);
            while (continuePoint.getKey() < 0) {
                boolean success = indexReader.changeToPre();
                if(!success) {
                    break;
                }
                continuePoint = indexReader.seek(request);
            }
            baseOffset = indexReader.getStartOffset();

            if(continuePoint.getKey() >= 0) {
                continueGtidSet = continuePoint.getValue();
                offset = continuePoint.getKey();
            }
        }
        offset = Math.max(offset, 0);


        return new Pair<>(baseOffset + offset, continueGtidSet);
    }

    public GtidSet getIndexGtidSet() {
        return indexWriter.getGtidSet();
    }

    public void buildIndexFromCmdFile(String cmdFileName, long cmdFileOffset) throws IOException {

        this.streamCommandReader = new StreamCommandReader(cmdFileOffset, this.opParser);
        this.streamCommandReader.addListener(this);

        File f = new File(baseDir + cmdFileName);
        ControllableFile controllableFile = new DefaultControllableFile(f);
        controllableFile.getFileChannel().position(cmdFileOffset);
        while(controllableFile.getFileChannel().position() < controllableFile.getFileChannel().size()) {
            int size = (int)Math.min(1024*8, controllableFile.getFileChannel().size() - controllableFile.getFileChannel().position());
            ByteBuffer buffer = ByteBuffer.allocate(size);
            controllableFile.getFileChannel().read(buffer);
            buffer.flip();
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
            this.write(byteBuf);
        }
    }

    @Override
    public void close() throws IOException {
        this.indexWriter.close();
    }

    public void closeWithDeleteIndexFiles() throws IOException {
        this.close();
        File directory = new File(baseDir);

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
}
