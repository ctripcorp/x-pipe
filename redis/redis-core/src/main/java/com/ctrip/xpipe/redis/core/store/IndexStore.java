package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

public interface IndexStore {

    void write(ByteBuf byteBuf) throws IOException;
    void rotateFileIfNecessary() throws IOException;
    void openWriter(CommandWriter cmdWriter) throws IOException;
    List<Pair<Long,  Long>> locateGtidRange(String uuid, int begGno, int endGno) throws IOException;
    Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws IOException;
    Pair<Long, GtidSet> locateGtidSetWithFallbackToEnd(GtidSet request) throws IOException;
    boolean increaseLost(GtidSet lost, IOSupplier<Boolean> supplier) throws IOException;
    Pair<Long, GtidSet> locateTailOfCmd();
    GtidSet getIndexGtidSet();
    void closeWithDeleteIndexFiles() throws IOException;
    void closeWriter() throws IOException;
    void resetParserState();
}
