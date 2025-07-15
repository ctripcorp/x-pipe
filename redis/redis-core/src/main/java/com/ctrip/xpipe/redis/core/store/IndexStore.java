package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

public interface IndexStore extends Closeable {

    void write(ByteBuf byteBuf) throws IOException;
    void rotateFileIfNecessary() throws IOException;
    void initialize(CommandWriter cmdWriter) throws IOException;
    Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws IOException;
    Pair<Long, GtidSet> locateGtidSetWithFallbackToEnd(GtidSet request) throws IOException;
    boolean increaseLost(GtidSet lost, IOSupplier<Boolean> supplier) throws IOException;
    Pair<Long, GtidSet> locateTailOfCmd();
    GtidSet getIndexGtidSet();
    void closeWithDeleteIndexFiles() throws IOException;
}
