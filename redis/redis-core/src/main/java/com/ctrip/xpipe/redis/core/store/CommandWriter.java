package com.ctrip.xpipe.redis.core.store;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/15
 */
public interface CommandWriter extends Closeable {

    void initialize() throws IOException;

    int write(ByteBuf byteBuf) throws IOException;

    boolean rotateFileIfNecessary() throws IOException;

    long totalLength();

    long fileLength();

    long getFileLastModified();

    CommandFileContext getFileContext();
}
