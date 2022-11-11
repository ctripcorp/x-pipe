package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.utils.CloseState;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.ctrip.xpipe.utils.OsUtils.LINE_SEPARATOR;

/**
 * @author Slight
 * <p>
 * Nov 07, 2022 20:39
 */
public class Gtid2OffsetIndexGenerator implements CommandsListener {

    private static final Logger logger = LoggerFactory.getLogger(Gtid2OffsetIndexGenerator.class);

    private final CommandStore cmdStore;

    private final int maxFileSize;

    private CloseState closeState = new CloseState();

    private GtidSet gtid_received;

    private volatile GtidSet endGtidSet;

    private CommandFile currentFile;

    private ControllableFile indexControllableFile;

    public Gtid2OffsetIndexGenerator(CommandStore cmdStore, int maxFileSize, GtidSet initGtidSet) {
        this.cmdStore = cmdStore;
        this.maxFileSize = maxFileSize;
        this.gtid_received = initGtidSet;
    }

    @Override
    public boolean isOpen() {
        return closeState.isOpen();
    }

    @Override
    public ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd) {

        if (!(cmd instanceof RedisOp)) {
            throw new UnsupportedOperationException();
        }

        RedisOp redisOp = (RedisOp) cmd;

        String cmdGtid = redisOp.getOpGtid();

        gtid_received.add(cmdGtid);

        //avoid currency-problem
        //TODO: enhance performance
        endGtidSet = gtid_received.clone();

        try {

            rotateIndexFileIfNecessary(currentFile);

            if (shouldInsert()) {
                CommandFileOffsetGtidIndex index = new CommandFileOffsetGtidIndex(gtid_received, currentFile, filePosition);
                tryInsertIndex(index);
            }

        } catch (IOException ignore) {
            logger.warn("[onCommand] maybe, DefaultControllableFile is not created.", ignore);
        }

        return null;
    }

    private void rotateIndexFileIfNecessary(CommandFile comingFile) throws IOException {

        if (comingFile.equals(currentFile)) {
            return;
        }

        tryCloseFile(indexControllableFile);

        File indexFile = getCommandStore().findIndexFile(comingFile);
        indexControllableFile = new DefaultControllableFile(indexFile);
        currentFile = comingFile;
    }

    private boolean shouldInsert() {
        //TODO
        return true;
    }

    private void tryInsertIndex(CommandFileOffsetGtidIndex index) {
        try {
            //generator
            this.indexControllableFile.getFileChannel().write(ByteBuffer.wrap((index.buildIdxStr() + LINE_SEPARATOR).getBytes()));
        } catch (Throwable throwable) {
            logger.info("[tryInsertIndex][fail] {}", index, throwable);
        }
        getCommandStore().addIndex(index);
    }

    private void tryCloseFile(ControllableFile file) {
        if (null == file) return;

        try {
            logger.debug("[tryCloseFile]{}", file);
            file.close();
        } catch (IOException e) {
            logger.error("[tryCloseFile]" + file, e);
        }
    }

    private CommandStore getCommandStore() {
        return cmdStore;
    }

    @Override
    public void beforeCommand() {
        //doNothing
    }

    @Override
    public Long processedOffset() {
        return Long.MAX_VALUE;
    }

    public GtidSet getEndGtidSet() {
        return endGtidSet;
    }
}
