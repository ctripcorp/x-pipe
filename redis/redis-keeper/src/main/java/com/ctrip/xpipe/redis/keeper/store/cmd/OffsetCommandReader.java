package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * @author lishanglin
 * date 2022/4/17
 */
public class OffsetCommandReader extends AbstractFlyingThresholdCommandReader<ReferenceFileRegion> {

    private long curPosition;

    private long endPositionExcluded;

    private CommandStore commandStore;

    private AsyncCommandStore asyncCommandStore;

    private final AsyncSegmentFile readAsyncSegmentFile;

    private OffsetNotifier offsetNotifier;

    private ReplDelayConfig replDelayConfig;

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandReader.class);

    public OffsetCommandReader(long globalPosition, long endPositionExcluded, CommandStore commandStore,
                               OffsetNotifier offsetNotifier, ReplDelayConfig replDelayConfig, long flyingThreshold)
            throws IOException {
        super(commandStore, flyingThreshold);
        this.commandStore = commandStore;
        this.offsetNotifier = offsetNotifier;
        this.asyncCommandStore = (AsyncCommandStore) commandStore;
        this.replDelayConfig = replDelayConfig;
        this.curPosition = globalPosition;
        this.endPositionExcluded = endPositionExcluded;
        this.readAsyncSegmentFile = openReadAsyncSegmentFile();
    }

    private AsyncSegmentFile openReadAsyncSegmentFile() throws IOException {
        AsyncFileSystem asyncFileSystem = asyncCommandStore.getAsyncFileSystem();
        AsyncSegmentFile file = AsyncFileSystemHelper.await(
                asyncFileSystem.open(
                        asyncCommandStore.getCommandBaseDir().getAbsolutePath(),
                        asyncCommandStore.getCommandFileNamePrefix(),
                        asyncCommandStore.getCommandIndexFileMappings(),
                        false),
                "open command segment for read");
        return file;
    }

    @Override
    public ReferenceFileRegion doRead(long milliSeconds) throws IOException {
        try {
            if (milliSeconds < 0) offsetNotifier.await(curPosition);
            else offsetNotifier.await(curPosition, milliSeconds);
        } catch (InterruptedException e) {
            logger.info("[read]", e);
            Thread.currentThread().interrupt();
            return null;
        }

        commandStore.makeSureOpen();

        long readableBytes = commandStore.totalLength() - curPosition;
        if (readableBytes <= 0) return null;

        long limitBytes = replDelayConfig.getPsyncLimitPerSecond();
        if (endPositionExcluded > 0) {
            if (endPositionExcluded == curPosition) return ReferenceFileRegion.EOF;
            long bytesToEnd = endPositionExcluded - curPosition;
            if (limitBytes < 0 || bytesToEnd < limitBytes) limitBytes = bytesToEnd;
        }
        if (limitBytes < 0 || readableBytes < limitBytes) limitBytes = readableBytes;

        ReferenceFileRegion referenceFileRegion = new AsyncReferenceFileRegion(asyncCommandStore.getAsyncFileSystem(),
                readAsyncSegmentFile, curPosition, limitBytes);

        curPosition += referenceFileRegion.count();

        referenceFileRegion.setTotalPos(curPosition);

        if (referenceFileRegion.count() < 0) {
            logger.error("[read]{}", referenceFileRegion);
        }

        return referenceFileRegion;
    }

    @Override
    public CommandFile getCurCmdFile() {
        try {
            return commandStore.findFileForOffset(curPosition);
        } catch (IOException e) {
            throw new IllegalStateException("failed to find command file for offset " + curPosition, e);
        }
    }

    @Override
    public void close() throws IOException {
        AsyncFileSystemHelper.await(
                asyncCommandStore.getAsyncFileSystem().close(readAsyncSegmentFile),
                "close read command segment");
        commandStore.removeReader(this);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public String toString() {
        CommandFile curCmdFile = getCurCmdFile();
        return "curFile:" + (curCmdFile == null ? "null" : curCmdFile.getFile());
    }

}
