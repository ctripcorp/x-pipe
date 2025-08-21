package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandFileOffsetGtidIndex;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.utils.CloseState;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
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

    private static final int DEFAULT_BYTES_BETWEEN_INDEX = 50 * 1024 * 1024;

    private static final int DEFAULT_LWM_DISTANCE_BETWEEN_INDEX = 1024;

    private CloseState closeState = new CloseState();

    private GtidSet gtid_received;

    private GtidSet last_indexed_gtid;

    private volatile GtidSet endGtidSet;

    private CommandFile currentFile;

    private ControllableFile indexControllableFile;

    private EmbeddedChannel channel = new EmbeddedChannel();

    public Gtid2OffsetIndexGenerator(CommandStore cmdStore, GtidSet initGtidSet) {
        this.cmdStore = cmdStore;
        this.gtid_received = initGtidSet;
        this.last_indexed_gtid = gtid_received.clone();
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

        try {

            rotateIndexFileIfNecessary(currentFile);

            if(!gtid_received.add(cmdGtid)) {
                //coming gtid already disposed of
                return channel.newSucceededFuture();
            }

            //avoid currency-problem
            endGtidSet = gtid_received.clone();

            if (shouldInsert()) {
                CommandFileOffsetGtidIndex index = new CommandFileOffsetGtidIndex(gtid_received.clone(), currentFile, filePosition);
                tryInsertIndex(index);
            }

        } catch (IOException ignore) {
            logger.warn("[onCommand] maybe, DefaultControllableFile is not created.", ignore);
        }

        return channel.newSucceededFuture();
    }

    private void rotateIndexFileIfNecessary(CommandFile comingFile) throws IOException {

        if (comingFile.equals(currentFile)) {
            return;
        }

        tryCloseFile(indexControllableFile);

        File indexFile = cmdStore.findIndexFile(comingFile);
        indexControllableFile = new DefaultControllableFile(indexFile);
        currentFile = comingFile;
    }

    private boolean shouldInsert() {
        if (gtid_received.lwmDistance(last_indexed_gtid) < DEFAULT_LWM_DISTANCE_BETWEEN_INDEX) {
            if (logger.isDebugEnabled()) {
                logger.debug("shouldInsert = false, gtid_received: {}, last_indexed_gtid: {}", gtid_received.toString(), last_indexed_gtid.toString());
            }
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("shouldInsert = true, gtid_received: {}, last_indexed_gtid: {}", gtid_received.toString(), last_indexed_gtid.toString());
        }
        last_indexed_gtid = gtid_received.clone();
        return true;
    }

    private void tryInsertIndex(CommandFileOffsetGtidIndex index) {
        try {
            //generator
            this.indexControllableFile.getFileChannel().write(ByteBuffer.wrap((index.buildIdxStr() + LINE_SEPARATOR).getBytes()));
        } catch (Throwable throwable) {
            logger.info("[tryInsertIndex][fail] {}", index, throwable);
        }
        cmdStore.addIndex(index);
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
