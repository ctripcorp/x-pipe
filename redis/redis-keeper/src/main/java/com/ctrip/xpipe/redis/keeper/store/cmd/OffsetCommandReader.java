package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannel;
import com.ctrip.xpipe.netty.filechannel.DefaultReferenceFileRegion;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.OffsetNotifier;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/17
 */
public class OffsetCommandReader extends AbstractFlyingThresholdCommandReader<ReferenceFileRegion> implements CommandReader<ReferenceFileRegion> {

    private CommandFile curCmdFile;

    private long curPosition;

    private long endPositionExcluded;

    private ReferenceFileChannel referenceFileChannel;

    private CommandStore commandStore;

    private OffsetNotifier offsetNotifier;

    private ReplDelayConfig replDelayConfig;

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandReader.class);

    public OffsetCommandReader(CommandFile commandFile, long globalPosition, long endPositionExcluded, long filePosition, CommandStore commandStore,
                               OffsetNotifier offsetNotifier, ReplDelayConfig replDelayConfig, long flyingThreshold)
            throws IOException {
        super(commandStore, flyingThreshold);
        this.commandStore = commandStore;
        this.offsetNotifier = offsetNotifier;
        this.curCmdFile = commandFile;
        this.replDelayConfig = replDelayConfig;
        this.curPosition = globalPosition;
        this.endPositionExcluded = endPositionExcluded;
        referenceFileChannel = new ReferenceFileChannel(new DefaultControllableFile(curCmdFile.getFile()), filePosition);
    }

    @Override
    public ReferenceFileRegion doRead(long milliSeconds) throws IOException {
        try {
            if (milliSeconds < 0) offsetNotifier.await(curPosition);
            else offsetNotifier.await(curPosition, milliSeconds);
            readNextFileIfNecessary();
        } catch (InterruptedException e) {
            logger.info("[read]", e);
            Thread.currentThread().interrupt();
            return null;
        }

        if (!referenceFileChannel.hasAnythingToRead()) return null;
        long limitBytes = replDelayConfig.getLimitBytesPerSecond();
        if (endPositionExcluded > 0) {
            if (endPositionExcluded == curPosition) return ReferenceFileRegion.EOF;
            long bytesToEnd = endPositionExcluded - curPosition;
            if (limitBytes < 0 || bytesToEnd < limitBytes) limitBytes = bytesToEnd;
        }

        ReferenceFileRegion referenceFileRegion = referenceFileChannel.read(limitBytes);

        curPosition += referenceFileRegion.count();

        referenceFileRegion.setTotalPos(curPosition);

        if (referenceFileRegion.count() < 0) {
            logger.error("[read]{}", referenceFileRegion);
        }

        return referenceFileRegion;
    }

    private void readNextFileIfNecessary() throws IOException {
        commandStore.makeSureOpen();

        if (!referenceFileChannel.hasAnythingToRead()) {
            // TODO notify when next file ready
            CommandFile nextCommandFile = commandStore.findNextFile(curCmdFile.getFile());
            if (nextCommandFile != null) {
                curCmdFile = nextCommandFile;
                referenceFileChannel.close();
                referenceFileChannel = new ReferenceFileChannel(new DefaultControllableFile(curCmdFile.getFile()));
            }
        }
    }

    @Override
    public long position() throws IOException {
        return referenceFileChannel.position();
    }

    @Override
    public CommandFile getCurCmdFile() {
        return curCmdFile;
    }

    @Override
    public void close() throws IOException {
        commandStore.removeReader(this);
        referenceFileChannel.close();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public String toString() {
        return "curFile:" + curCmdFile.getFile();
    }

    @VisibleForTesting
    protected void setFileChannel(ReferenceFileChannel fileChannel) {
        this.referenceFileChannel = fileChannel;
    }

}
