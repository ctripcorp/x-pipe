package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface IndexStore extends Closeable {

    void write(ByteBuf byteBuf) throws IOException;
    void doRotate() throws IOException;
    boolean needRotate();
    void openWriter(CommandWriter cmdWriter) throws IOException;

    /**
     * After rotate-failure unbind: if writers are null, rebind to the current tip using the
     * snapshot continueGtidSet. Already bound → no-op. Failures propagate (T-H3.CP3).
     */
    void rebindWritersToCurrentTipIfUnbound() throws IOException;

    /**
     * Atomically rotate cmd segment + index writers under the IndexStore monitor.
     * Order: {@link #flushWriter()} → {@code cmdRoll} (typically {@code CommandWriter#doRotate}/fs.roll)
     * → rebind index writers to the new segment. Callers must not split cmd roll and index
     * rebind outside this lock — concurrent locate may otherwise see tip empty index and
     * fall back to {@code locateTailOfCmd} (skip old-segment GTIDs).
     */
    void rotateWithCmdRoll(IOSupplier<?> cmdRoll) throws IOException;
    List<Pair<Long,  Long>> locateGtidRange(String uuid, long begGno, long endGno) throws IOException;
    Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws IOException;
    Pair<Long, GtidSet> locateGtidSetWithFallbackToEnd(GtidSet request) throws IOException;
    boolean increaseLost(GtidSet lost, IOSupplier<Boolean> supplier) throws IOException;
    Pair<Long, GtidSet> locateTailOfCmd();
    GtidSet getIndexGtidSet();

    /**
     * Flush pending index/block entries for the current segment before {@code fs.roll}.
     * Must not reset {@code StreamCommandParser} — incomplete RESP may span the rotate.
     */
    void flushWriter() throws IOException;

    /**
     * Flush writers and reset parser. Use for protocol switch only
     * ({@code switchToXSync} / {@code switchToPsync}) — does <em>not</em> mark IndexStore closed;
     * ordinary segment rotate must use {@link #flushWriter()}; store teardown must use {@link #close()}.
     */
    void closeWriter() throws IOException;

    /**
     * Terminal close: {@link #closeWriter()} + drop writer references + mark closed.
     * After close, {@link #openWriter} and write paths must fail.
     * Does <em>not</em> {@code fs.close} index {@code AsyncFile} handles — those are owned by the
     * write-mode segment and released when CmdStore closes the segment.
     */
    @Override
    void close() throws IOException;

    void resetParserState();
}
