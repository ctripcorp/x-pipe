package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
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
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author lishanglin
 * date 2022/4/17
 */
public class OffsetCommandReader extends AbstractFlyingThresholdCommandReader<ReferenceFileRegion> {

    /**
     * Scan cursor: next logical offset to emit as a {@link AsyncReferenceFileRegion}.
     * Advanced when a region is created, before Netty {@code transferTo} actually reads bytes.
     */
    private long curPosition;

    private long endPositionExcluded;

    private CommandStore commandStore;

    private AsyncCommandStore asyncCommandStore;

    private final AsyncSegmentFile readAsyncSegmentFile;

    private OffsetNotifier offsetNotifier;

    private ReplDelayConfig replDelayConfig;

    /**
     * In-flight regions awaiting Netty flush, in emit order.
     * GC {@link #getReadOffset()} uses the oldest region's {@link AsyncReferenceFileRegion#getCurrentReadOffset()}.
     */
    private final ConcurrentLinkedQueue<AsyncReferenceFileRegion> flyingRegions = new ConcurrentLinkedQueue<>();

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
        AsyncSegmentFile file = AsyncFileSystemHelper.awaitOpen(asyncFileSystem,
                asyncFileSystem.open(
                        asyncCommandStore.getCommandBaseDir().getAbsolutePath(),
                        asyncCommandStore.getCommandFileNamePrefix(),
                        asyncCommandStore.getCommandIndexPrefixes(),
                        false,
                        asyncCommandStore.getFileSystemReplId().toString()),
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

        AsyncReferenceFileRegion referenceFileRegion = new AsyncReferenceFileRegion(
                asyncCommandStore.getAsyncFileSystem(), readAsyncSegmentFile, curPosition, limitBytes);

        flyingRegions.add(referenceFileRegion);
        curPosition += referenceFileRegion.count();

        referenceFileRegion.setTotalPos(curPosition);

        if (referenceFileRegion.count() < 0) {
            logger.error("[read]{}", referenceFileRegion);
        }

        return referenceFileRegion;
    }

    @Override
    protected void onFlushed(ReferenceFileRegion cmdContent) {
        if (cmdContent instanceof AsyncReferenceFileRegion) {
            flyingRegions.remove(cmdContent);
        }
    }

    /**
     * GC lowest-read gate: offset already delivered by {@code transferTo}, not the scan cursor.
     * <p>
     * {@link #curPosition} advances when a region is emitted; actual bytes are read later on the
     * Netty thread via {@link AsyncReferenceFileRegion#transferTo}. Using curPosition as the gate
     * would allow GC to delete unread backlog.
     */
    @Override
    public long getReadOffset() {
        AsyncReferenceFileRegion oldest = flyingRegions.peek();
        if (oldest != null) {
            return oldest.getCurrentReadOffset();
        }
        return curPosition;
    }

    @Override
    public long getCurStartOffset() {
        // transferTo does not update AsyncSegmentFile.position (FS contract, like Linux);
        // map actual transfer progress to segment start via FS.
        return asyncCommandStore.getAsyncFileSystem()
                .getStartOffsetByReadOffset(readAsyncSegmentFile, getReadOffset());
    }

    @Override
    public void close() throws IOException {
        flyingRegions.clear();
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
        return "curStartOffset:" + getCurStartOffset() + ", readOffset:" + getReadOffset();
    }

}
