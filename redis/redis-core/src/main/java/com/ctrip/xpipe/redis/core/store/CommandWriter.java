package com.ctrip.xpipe.redis.core.store;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/15
 */
public interface CommandWriter extends Closeable {

    void rotateFileIfNecessary() throws IOException;

    int write(ByteBuf byteBuf) throws IOException;

    long totalLength();

    long getFileLastModified();
}
