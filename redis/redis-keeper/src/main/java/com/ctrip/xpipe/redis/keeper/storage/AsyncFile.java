package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;

public class AsyncFile extends AbstractStorageFile {

    final String path;
    FileChannel channel;
    final boolean atomicReplace;
    long position = 0;

    AsyncFile(String path, FileChannel channel, boolean atomicReplace, boolean writeMode) {
        super(writeMode);
        this.path = path;
        this.channel = channel;
        this.atomicReplace = atomicReplace;
    }

    @Override
    FileChannel currentWriteChannel() {
        return channel;
    }

    @Override
    String identifier() {
        return path;
    }
}
