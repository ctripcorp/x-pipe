package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.core.store.CommandFileContext;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/4/15
 */
public class OffsetCommandWriter implements CommandWriter, OffsetNotifyingCommandWriter {

    private CommandStore cmdStore;

    private AsyncCommandStore asyncCommandStore;

    private int maxFileSize;

    private Logger delayTraceLogger;

    private volatile OffsetNotifier offsetNotifier;

    private volatile boolean initialized;

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandWriter.class);

    public OffsetCommandWriter(CommandStore cmdStore, int maxFileSize,
                               Logger delayTraceLogger) {
        this.cmdStore = cmdStore;
        this.asyncCommandStore = (AsyncCommandStore) Objects.requireNonNull(cmdStore, "cmdStore");
        this.maxFileSize = maxFileSize;
        this.delayTraceLogger = delayTraceLogger;
    }

    @Override
    public void initialize() throws IOException {
        if (initialized) return;

        long startOffset = currentSegmentStartOffset();
        initialized = true;
        logger.info("[initialize] write from segment start offset {}", startOffset);
    }

    @Override
    public int write(ByteBuf byteBuf) throws IOException {
        if(delayTraceLogger.isDebugEnabled()) {
            delayTraceLogger.debug("[appendCommands][begin]{}");
        }

        int wrote = 0;
        while (byteBuf.isReadable()) {
            int chunkLength = nextChunkLength(byteBuf.readableBytes(), currentSegmentSize());
            ByteBuf chunk = byteBuf.readRetainedSlice(chunkLength);

            long expectedEndOffset = totalLength() + chunkLength - 1;
            try {
                AsyncFileSystemHelper.writeAndAwait(asyncFileSystem(), asyncSegmentFile(), chunk, chunkLength,
                        "write command segment offset " + expectedEndOffset);
            } catch (IOException e) {
                onAsyncWriteFailure(expectedEndOffset, e);
                throw e;
            }
            wrote += chunkLength;
            notifyOffset(expectedEndOffset);
        }

        if(delayTraceLogger.isDebugEnabled()){
            logger.debug("[appendCommands][segmentStartOffset={}] fileLength={}",
                    currentSegmentStartOffset(), fileLength());
        }

        long offset = totalLength() - 1;
        if(delayTraceLogger.isDebugEnabled()){
            delayTraceLogger.debug("[appendCommands][ end ]{}", offset + 1);
        }

        return wrote;
    }

    private int nextChunkLength(int readableBytes, long currentFileLength) {
        int maxWriteBytes = asyncCommandStore.getAsyncWriteMaxBytes();
        long fileRemaining = maxFileSize - currentFileLength;
        if (fileRemaining <= 0) {
            return Math.min(readableBytes, maxWriteBytes);
        }
        return (int) Math.min(Math.min(readableBytes, maxWriteBytes), fileRemaining);
    }

    private void notifyOffset(long offset) {
        OffsetNotifier notifier = offsetNotifier;
        if (notifier != null) {
            notifier.offsetIncreased(offset);
        }
    }

    protected void onAsyncWriteFailure(long offset, IOException e) {
        logger.error("[asyncWrite][fail][offset={}][fallback pending]", offset, e);
    }

    @Override
    public boolean needRotate() throws IOException {
        return currentSegmentSize() >= maxFileSize;
    }

    @Override
    public void doRotate() throws IOException {
        logger.info("Rotate command segment at offset {}", totalLength());
        AsyncFileSystemHelper.await(asyncFileSystem().roll(asyncSegmentFile()), "roll command segment");
    }

    @Override
    public synchronized long totalLength() {
        try {
            return currentSegmentStartOffset() + currentSegmentSize();
        } catch (IOException e) {
            throw new IllegalStateException("failed to query command segment total length", e);
        }
    }

    @Override
    public long fileLength() {
        try {
            return currentSegmentSize();
        } catch (IOException e) {
            throw new IllegalStateException("failed to query command segment file length", e);
        }
    }

    @Override
    public long getFileLastModified() {
        try {
            return AsyncFileSystemHelper.await(asyncFileSystem().lastModified(asyncSegmentFile()),
                    "lastModified command segment");
        } catch (IOException e) {
            throw new IllegalStateException("failed to query command segment lastModified", e);
        }
    }

    @Override
    public CommandFileContext getFileContext() {
        try {
            return new CommandFileContext(currentCommandFile());
        } catch (IOException e) {
            throw new IllegalStateException("failed to build command file context", e);
        }
    }

    @Override
    public void close() throws IOException {
        // segment 句柄由 CommandStore.close() 统一 fs.close(seg)
    }

    @Override
    public void setOffsetNotifier(OffsetNotifier offsetNotifier) {
        this.offsetNotifier = offsetNotifier;
    }

    private AsyncFileSystem asyncFileSystem() {
        return asyncCommandStore.getAsyncFileSystem();
    }

    private AsyncSegmentFile asyncSegmentFile() {
        return asyncCommandStore.getAsyncSegmentFile();
    }

    private long currentSegmentStartOffset() {
        long startOffset = asyncFileSystem().getCurrentSegmentStartOffset(asyncSegmentFile());
        if (startOffset >= 0) {
            return startOffset;
        }
        // FS may return -1 when write channel is temporarily closed; fall back to latest segment.
        List<Long> segmentOffsets = asyncFileSystem().list(asyncSegmentFile());
        if (segmentOffsets.isEmpty()) {
            return 0;
        }
        return segmentOffsets.get(segmentOffsets.size() - 1);
    }

    private long currentSegmentSize() throws IOException {
        long startOffset = currentSegmentStartOffset();
        if (0 == startOffset) {
            return AsyncFileSystemHelper.await(
                    asyncFileSystem().size(asyncSegmentFile()),
                    "sizeOfSegment");
        } else {
            return AsyncFileSystemHelper.await(
                    asyncFileSystem().sizeOfSegment(asyncSegmentFile(), startOffset),
                    "sizeOfSegment start " + startOffset);
        }
    }

    private CommandFile currentCommandFile() {
        long startOffset = currentSegmentStartOffset();
        File file = new File(asyncCommandStore.getCommandBaseDir(),
                asyncCommandStore.getCommandFileNamePrefix() + startOffset);
        return new CommandFile(file, startOffset);
    }
}
