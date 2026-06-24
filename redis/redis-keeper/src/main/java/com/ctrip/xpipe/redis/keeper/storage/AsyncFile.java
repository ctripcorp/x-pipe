package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;

public class AsyncFile {

    final String path;
    FileChannel channel;
    final boolean atomicReplace;

    AsyncFile(String path, FileChannel channel) {
        this(path, channel, false);
    }

    AsyncFile(String path, FileChannel channel, boolean atomicReplace) {
        this.path = path;
        this.channel = channel;
        this.atomicReplace = atomicReplace;
    }
}
